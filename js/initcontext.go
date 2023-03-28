package js

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/dop251/goja"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"

	"go.k6.io/k6/lib/fsext"
	"go.k6.io/k6/loader"
)

const cantBeUsedOutsideInitContextMsg = `the "%s" function is only available in the init stage ` +
	`(i.e. the global scope), see https://k6.io/docs/using-k6/test-life-cycle for more information`

// openImpl implements openImpl() in the init context and will read and return the
// contents of a file. If the second argument is "b" it returns an ArrayBuffer
// instance, otherwise a string representation.
func openImpl(rt *goja.Runtime, fs fsext.Fs, basePWD *url.URL, filename string, args ...string) (goja.Value, error) {
	// Here IsAbs should be enough but unfortunately it doesn't handle absolute paths starting from
	// the current drive on windows like `\users\noname\...`. Also it makes it more easy to test and
	// will probably be need for archive execution under windows if always consider '/...' as an
	// absolute path.
	if filename[0] != '/' && filename[0] != '\\' && !filepath.IsAbs(filename) {
		filename = filepath.Join(basePWD.Path, filename)
	}
	filename = filepath.Clean(filename)

	if filename[0:1] != fsext.FilePathSeparator {
		filename = fsext.FilePathSeparator + filename
	}

	data, err := readFile(fs, filename)
	if err != nil {
		return nil, err
	}

	if len(args) > 0 && args[0] == "b" {
		ab := rt.NewArrayBuffer(data)
		return rt.ToValue(&ab), nil
	}
	return rt.ToValue(string(data)), nil
}

func readFile(fileSystem fsext.Fs, filename string) (data []byte, err error) {
	defer func() {
		if errors.Is(err, fsext.ErrPathNeverRequestedBefore) {
			// loading different files per VU is not supported, so all files should are going
			// to be used inside the scenario should be opened during the init step (without any conditions)
			err = fmt.Errorf(
				"open() can't be used with files that weren't previously opened during initialization (__VU==0), path: %q",
				filename,
			)
		}
	}()

	// Workaround for https://github.com/spf13/fsext/issues/201
	if isDir, err := fsext.IsDir(fileSystem, filename); err != nil {
		return nil, err
	} else if isDir {
		return nil, fmt.Errorf("open() can't be used with directories, path: %q", filename)
	}

	return fsext.ReadFile(fileSystem, filename)
}

// allowOnlyOpenedFiles enables seen only files
func allowOnlyOpenedFiles(fs fsext.Fs) {
	alreadyOpenedFS, ok := fs.(fsext.OnlyCachedEnabler)
	if !ok {
		return
	}

	alreadyOpenedFS.AllowOnlyCached()
}

func generateSourceMapLoader(logger logrus.FieldLogger, filesystems map[string]afero.Fs,
) func(path string) ([]byte, error) {
	return func(path string) ([]byte, error) {
		u, err := url.Parse(path)
		if err != nil {
			return nil, err
		}
		data, err := loader.Load(logger, filesystems, u, path)
		if err != nil {
			return nil, err
		}
		return data.Data, nil
	}
}
