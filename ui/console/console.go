package console

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/fatih/color"
	"github.com/mattn/go-isatty"
	"github.com/sirupsen/logrus"
	"golang.org/x/term"

	"gopkg.in/yaml.v3"

	"go.k6.io/k6/lib"
	"go.k6.io/k6/ui/console/progressbar"
)

// Console enables synced writing to stdout and stderr ...
type Console struct {
	IsTTY          bool
	writeMx        *sync.Mutex
	Stdout, Stderr io.Writer
	Stdin          io.Reader
	quiet          bool
	theme          *theme
	logger         *logrus.Logger
}

func New(quiet, colorize bool) *Console {
	writeMx := &sync.Mutex{}
	stdout := newConsoleWriter(os.Stdout, writeMx)
	stderr := newConsoleWriter(os.Stderr, writeMx)
	isTTY := stdout.isTTY && stderr.isTTY

	// Default logger without any formatting
	logger := &logrus.Logger{
		Out:       stderr,
		Formatter: new(logrus.TextFormatter), // no fancy formatting here
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.InfoLevel,
	}

	var th *theme
	// Only enable themes and a fancy logger if we're in a TTY
	if isTTY && colorize {
		th = &theme{foreground: getColor(color.FgCyan)}

		logger = &logrus.Logger{
			Out: stderr,
			Formatter: &logrus.TextFormatter{
				ForceColors:   true,
				DisableColors: false,
			},
			Hooks: make(logrus.LevelHooks),
			Level: logrus.InfoLevel,
		}
	}

	return &Console{
		IsTTY:   isTTY,
		writeMx: writeMx,
		Stdout:  stdout,
		Stderr:  stderr,
		Stdin:   os.Stdin,
		theme:   th,
		logger:  logger,
	}
}

func (c *Console) Logger() *logrus.Logger {
	return c.logger
}

type theme struct {
	foreground *color.Color
}

// A writer that syncs writes with a mutex and, if the output is a TTY, clears before newlines.
type consoleWriter struct {
	out   io.Writer
	isTTY bool
	mutex *sync.Mutex

	// Used for flicker-free persistent objects like the progressbars
	persistentText func()
}

type osFile interface {
	io.Writer
	Fd() uintptr
}

func newConsoleWriter(out osFile, mx *sync.Mutex) *consoleWriter {
	isDumbTerm := os.Getenv("TERM") == "dumb"
	isTTY := !isDumbTerm && (isatty.IsTerminal(out.Fd()) || isatty.IsCygwinTerminal(out.Fd()))
	return &consoleWriter{out, isTTY, mx, nil}
}

func (w *consoleWriter) Write(p []byte) (n int, err error) {
	origLen := len(p)
	if w.isTTY {
		// Add a TTY code to erase till the end of line with each new line
		// TODO: check how cross-platform this is...
		p = bytes.ReplaceAll(p, []byte{'\n'}, []byte{'\x1b', '[', '0', 'K', '\n'})
	}

	w.mutex.Lock()
	n, err = w.out.Write(p)
	if w.persistentText != nil {
		w.persistentText()
	}
	w.mutex.Unlock()

	if err != nil && n < origLen {
		return n, err
	}
	return origLen, err
}

// getColor returns the requested color, or an uncolored object, depending on
// the value of noColor. The explicit EnableColor() and DisableColor() are
// needed because the library checks os.Stdout itself otherwise...
func getColor(attributes ...color.Attribute) *color.Color {
	// if noColor {
	// 	c := color.New()
	// 	c.DisableColor()
	// 	return c
	// }

	c := color.New(attributes...)
	c.EnableColor()
	return c
}

func (c *Console) ApplyTheme(s string) string {
	if c.theme != nil {
		return c.theme.foreground.Sprint(s)
	}

	return s
}

func (c *Console) Print(s string) {
	if _, err := fmt.Fprint(c.Stdout, s); err != nil {
		c.logger.Errorf("could not print '%s' to stdout: %s", s, err.Error())
	}
}

func (c *Console) PrintBanner() {
	_, err := fmt.Fprintf(c.Stdout, "\n%s\n\n", c.ApplyTheme(banner))
	if err != nil {
		c.logger.Warnf("could not print k6 banner message to stdout: %s", err.Error())
	}
}

func (c *Console) PrintBar(pb *progressbar.ProgressBar) {
	end := "\n"
	// TODO: refactor widthDelta away? make the progressbar rendering a bit more
	// stateless... basically first render the left and right parts, so we know
	// how long the longest line is, and how much space we have for the progress
	widthDelta := -defaultTermWidth
	if c.IsTTY {
		// If we're in a TTY, instead of printing the bar and going to the next
		// line, erase everything till the end of the line and return to the
		// start, so that the next print will overwrite the same line.
		//
		// TODO: check for cross platform support
		end = "\x1b[0K\r"
		widthDelta = 0
	}
	rendered := pb.Render(0, widthDelta)
	// Only output the left and middle part of the progress bar
	c.Print(rendered.String() + end)
}

func (c *Console) ModifyAndPrintBar(bar *progressbar.ProgressBar, options ...progressbar.ProgressBarOption) {
	bar.Modify(options...)
	c.PrintBar(bar)
}

//nolint:funlen
func renderMultipleBars(
	nocolor, isTTY, goBack bool, maxLeft, termWidth, widthDelta int, pbs []*pb.ProgressBar,
) (string, int) {
	lineEnd := "\n"
	if isTTY {
		// TODO: check for cross platform support
		lineEnd = "\x1b[K\n" // erase till end of line
	}

	var (
		// Amount of times line lengths exceed termWidth.
		// Needed to factor into the amount of lines to jump
		// back with [A and avoid scrollback issues.
		lineBreaks  int
		longestLine int
		// Maximum length of each right side column except last,
		// used to calculate the padding between columns.
		maxRColumnLen = make([]int, 2)
		pbsCount      = len(pbs)
		rendered      = make([]pb.ProgressBarRender, pbsCount)
		result        = make([]string, pbsCount+2)
	)

	result[0] = lineEnd // start with an empty line

	// First pass to render all progressbars and get the maximum
	// lengths of right-side columns.
	for i, pb := range pbs {
		rend := pb.Render(maxLeft, widthDelta)
		for i := range rend.Right {
			// Skip last column, since there's nothing to align after it (yet?).
			if i == len(rend.Right)-1 {
				break
			}
			if len(rend.Right[i]) > maxRColumnLen[i] {
				maxRColumnLen[i] = len(rend.Right[i])
			}
		}
		rendered[i] = rend
	}

	// Second pass to render final output, applying padding where needed
	for i := range rendered {
		rend := rendered[i]
		if rend.Hijack != "" {
			result[i+1] = rend.Hijack + lineEnd
			runeCount := utf8.RuneCountInString(rend.Hijack)
			lineBreaks += (runeCount - termPadding) / termWidth
			continue
		}
		var leftText, rightText string
		leftPadFmt := fmt.Sprintf("%%-%ds", maxLeft)
		leftText = fmt.Sprintf(leftPadFmt, rend.Left)
		for i := range rend.Right {
			rpad := 0
			if len(maxRColumnLen) > i {
				rpad = maxRColumnLen[i]
			}
			rightPadFmt := fmt.Sprintf(" %%-%ds", rpad+1)
			rightText += fmt.Sprintf(rightPadFmt, rend.Right[i])
		}
		// Get visible line length, without ANSI escape sequences (color)
		status := fmt.Sprintf(" %s ", rend.Status())
		line := leftText + status + rend.Progress() + rightText
		lineRuneCount := utf8.RuneCountInString(line)
		if lineRuneCount > longestLine {
			longestLine = lineRuneCount
		}
		lineBreaks += (lineRuneCount - termPadding) / termWidth
		if !nocolor {
			rend.Color = true
			status = fmt.Sprintf(" %s ", rend.Status())
			line = fmt.Sprintf(leftPadFmt+"%s%s%s",
				rend.Left, status, rend.Progress(), rightText)
		}
		result[i+1] = line + lineEnd
	}

	if isTTY && goBack {
		// Clear screen and go back to the beginning
		// TODO: check for cross platform support
		result[pbsCount+1] = fmt.Sprintf("\r\x1b[J\x1b[%dA", pbsCount+lineBreaks+1)
	} else {
		result[pbsCount+1] = ""
	}

	return strings.Join(result, ""), longestLine
}

// TODO: show other information here?
// TODO: add a no-progress option that will disable these
// TODO: don't use global variables...
//nolint:funlen,gocognit
func showProgress(ctx context.Context, gs *globalState, pbs []*pb.ProgressBar, logger *logrus.Logger) {
	if gs.flags.quiet {
		return
	}

	var errTermGetSize bool
	termWidth := defaultTermWidth
	if gs.stdOut.isTTY {
		tw, _, err := term.GetSize(int(gs.stdOut.rawOut.Fd()))
		if !(tw > 0) || err != nil {
			errTermGetSize = true
			logger.WithError(err).Warn("error getting terminal size")
		} else {
			termWidth = tw
		}
	}

	// Get the longest left side string length, to align progress bars
	// horizontally and trim excess text.
	var leftLen int64
	for _, pb := range pbs {
		l := pb.Left()
		leftLen = lib.Max(int64(len(l)), leftLen)
	}
	// Limit to maximum left text length
	maxLeft := int(lib.Min(leftLen, maxLeftLength))

	var progressBarsLastRenderLock sync.Mutex
	var progressBarsLastRender []byte

	printProgressBars := func() {
		progressBarsLastRenderLock.Lock()
		_, _ = gs.stdOut.writer.Write(progressBarsLastRender)
		progressBarsLastRenderLock.Unlock()
	}

	var widthDelta int
	// Default to responsive progress bars when in an interactive terminal
	renderProgressBars := func(goBack bool) {
		barText, longestLine := renderMultipleBars(
			gs.flags.noColor, gs.stdOut.isTTY, goBack, maxLeft, termWidth, widthDelta, pbs,
		)
		widthDelta = termWidth - longestLine - termPadding
		progressBarsLastRenderLock.Lock()
		progressBarsLastRender = []byte(barText)
		progressBarsLastRenderLock.Unlock()
	}

	// Otherwise fallback to fixed compact progress bars
	if !gs.stdOut.isTTY {
		widthDelta = -pb.DefaultWidth
		renderProgressBars = func(goBack bool) {
			barText, _ := renderMultipleBars(gs.flags.noColor, gs.stdOut.isTTY, goBack, maxLeft, termWidth, widthDelta, pbs)
			progressBarsLastRenderLock.Lock()
			progressBarsLastRender = []byte(barText)
			progressBarsLastRenderLock.Unlock()
		}
	}

	// TODO: make configurable?
	updateFreq := 1 * time.Second
	var stdoutFD int
	if gs.ui.stdOut.isTTY {
		stdoutFD = int(gs.stdOut.rawOut.Fd())
		updateFreq = 100 * time.Millisecond
		gs.ui.setPersistentText(printProgressBars)
		defer gs.ui.setPersistentText(nil)
		// gs.outMutex.Lock()
		// gs.stdOut.persistentText = printProgressBars
		// gs.stdErr.persistentText = printProgressBars
		// gs.outMutex.Unlock()
		// defer func() {
		// 	gs.outMutex.Lock()
		// 	gs.stdOut.persistentText = nil
		// 	gs.stdErr.persistentText = nil
		// 	gs.outMutex.Unlock()
		// }()
	}

	var winch chan os.Signal
	if sig := getWinchSignal(); sig != nil {
		winch = make(chan os.Signal, 10)
		gs.signalNotify(winch, sig)
		defer gs.signalStop(winch)
	}

	ticker := time.NewTicker(updateFreq)
	ctxDone := ctx.Done()
	for {
		select {
		case <-ctxDone:
			renderProgressBars(false)
			gs.outMutex.Lock()
			printProgressBars()
			gs.outMutex.Unlock()
			return
		case <-winch:
			if gs.stdOut.isTTY && !errTermGetSize {
				// More responsive progress bar resizing on platforms with SIGWINCH (*nix)
				tw, _, err := term.GetSize(stdoutFD)
				if tw > 0 && err == nil {
					termWidth = tw
				}
			}
		case <-ticker.C:
			// Default ticker-based progress bar resizing
			if gs.stdOut.isTTY && !errTermGetSize && winch == nil {
				tw, _, err := term.GetSize(stdoutFD)
				if tw > 0 && err == nil {
					termWidth = tw
				}
			}
		}
		renderProgressBars(true)
		gs.ui.Print()
		gs.outMutex.Lock()
		printProgressBars()
		gs.outMutex.Unlock()
	}
}

func yamlPrint(w io.Writer, v interface{}) error {
	data, err := yaml.Marshal(v)
	if err != nil {
		return fmt.Errorf("could not marshal YAML: %w", err)
	}
	_, err = fmt.Fprint(w, string(data))
	if err != nil {
		return fmt.Errorf("could flush the data to the output: %w", err)
	}
	return nil
}