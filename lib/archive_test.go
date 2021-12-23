/*
 *
 * k6 - a next-generation load testing tool
 * Copyright (C) 2016 Load Impact
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package lib

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/guregu/null.v3"

	"go.k6.io/k6/lib/consts"
	"go.k6.io/k6/lib/fsext"
	"go.k6.io/k6/stats"
)

func TestNormalizeAndAnonymizePath(t *testing.T) {
	testdata := map[string]string{
		"/tmp":                            "/tmp",
		"/tmp/myfile.txt":                 "/tmp/myfile.txt",
		"/home/myname":                    "/home/nobody",
		"/home/myname/foo/bar/myfile.txt": "/home/nobody/foo/bar/myfile.txt",
		"/Users/myname/myfile.txt":        "/Users/nobody/myfile.txt",
		"/Documents and Settings/myname/myfile.txt":           "/Documents and Settings/nobody/myfile.txt",
		"\\\\MYSHARED\\dir\\dir\\myfile.txt":                  "/nobody/dir/dir/myfile.txt",
		"\\NOTSHARED\\dir\\dir\\myfile.txt":                   "/NOTSHARED/dir/dir/myfile.txt",
		"C:\\Users\\myname\\dir\\myfile.txt":                  "/C/Users/nobody/dir/myfile.txt",
		"D:\\Documents and Settings\\myname\\dir\\myfile.txt": "/D/Documents and Settings/nobody/dir/myfile.txt",
		"C:\\uSers\\myname\\dir\\myfile.txt":                  "/C/uSers/nobody/dir/myfile.txt",
		"D:\\doCUMENts aND Settings\\myname\\dir\\myfile.txt": "/D/doCUMENts aND Settings/nobody/dir/myfile.txt",
	}
	// TODO: fix this - the issue is that filepath.Clean replaces `/` with whatever the path
	// separator is on the current OS and as such this gets confused for shared folder on
	// windows :( https://github.com/golang/go/issues/16111
	if runtime.GOOS != "windows" {
		testdata["//etc/hosts"] = "/etc/hosts"
	}
	for from, to := range testdata {
		from, to := from, to
		t.Run("path="+from, func(t *testing.T) {
			res := NormalizeAndAnonymizePath(from)
			assert.Equal(t, to, res)
			assert.Equal(t, res, NormalizeAndAnonymizePath(res))
		})
	}
}

func makeMemMapFs(t *testing.T, input map[string][]byte) fsext.FS {
	t.Helper()

	fs := fsext.NewInMemoryFS()
	for path, data := range input {
		require.NoError(t, fs.WriteFile(path, data, 0o644))
	}
	return fs
}

func getMapKeys(m map[string]fsext.FS) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	return keys
}

func diffMapFilesystems(t *testing.T, first, second map[string]fsext.FS) {
	require.ElementsMatch(t, getMapKeys(first), getMapKeys(second),
		"fs map keys don't match %s, %s", getMapKeys(first), getMapKeys(second))
	for key, fs := range first {
		secondFs := second[key]
		diffFilesystems(t, fs, secondFs)
	}
}

func diffFilesystems(t *testing.T, first, second fsext.FS) {
	diffFilesystemsDir(t, first, second, "/")
}

func getInfoNames(infos []os.FileInfo) []string {
	names := make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.Name()
	}
	return names
}

func diffFilesystemsDir(t *testing.T, first, second fsext.FS, dirname string) {
	// TODO: change ot io.FS

	firstInfos, err := afero.ReadDir(first.Afero(), dirname)
	require.NoError(t, err, dirname)

	secondInfos, err := afero.ReadDir(first.Afero(), dirname)
	require.NoError(t, err, dirname)

	require.ElementsMatch(t, getInfoNames(firstInfos), getInfoNames(secondInfos), "directory: "+dirname)
	for _, info := range firstInfos {
		path := filepath.Join(dirname, info.Name())
		if info.IsDir() {
			diffFilesystemsDir(t, first, second, path)
			continue
		}
		firstData, err := first.ReadFile(path)
		require.NoError(t, err, path)

		secondData, err := second.ReadFile(path)
		require.NoError(t, err, path)

		assert.Equal(t, firstData, secondData, path)
	}
}

func TestArchiveReadWrite(t *testing.T) {
	t.Run("Roundtrip", func(t *testing.T) {
		arc1 := &Archive{
			Type:      "js",
			K6Version: consts.Version,
			Options: Options{
				VUs:        null.IntFrom(12345),
				SystemTags: &stats.DefaultSystemTagSet,
			},
			FilenameURL: &url.URL{Scheme: "file", Path: "/path/to/a.js"},
			Data:        []byte(`// a contents`),
			PwdURL:      &url.URL{Scheme: "file", Path: "/path/to"},
			Filesystems: map[string]fsext.FS{
				"file": makeMemMapFs(t, map[string][]byte{
					"/path/to/a.js":      []byte(`// a contents`),
					"/path/to/b.js":      []byte(`// b contents`),
					"/path/to/file1.txt": []byte(`hi!`),
					"/path/to/file2.txt": []byte(`bye!`),
				}),
				"https": makeMemMapFs(t, map[string][]byte{
					"/cdnjs.com/libraries/Faker":          []byte(`// faker contents`),
					"/github.com/loadimpact/k6/README.md": []byte(`README`),
				}),
			},
		}

		buf := bytes.NewBuffer(nil)
		require.NoError(t, arc1.Write(buf))

		arc1Filesystems := arc1.Filesystems
		arc1.Filesystems = nil

		arc2, err := ReadArchive(buf)
		require.NoError(t, err)

		arc2Filesystems := arc2.Filesystems
		arc2.Filesystems = nil
		arc2.Filename = ""
		arc2.Pwd = ""

		assert.Equal(t, arc1, arc2)

		diffMapFilesystems(t, arc1Filesystems, arc2Filesystems)
	})

	t.Run("Anonymized", func(t *testing.T) {
		testdata := []struct {
			Pwd, PwdNormAnon string
		}{
			{"/home/myname", "/home/nobody"},
			{filepath.FromSlash("/C:/Users/Administrator"), "/C/Users/nobody"},
		}
		for _, entry := range testdata {
			arc1 := &Archive{
				Type: "js",
				Options: Options{
					VUs:        null.IntFrom(12345),
					SystemTags: &stats.DefaultSystemTagSet,
				},
				FilenameURL: &url.URL{Scheme: "file", Path: fmt.Sprintf("%s/a.js", entry.Pwd)},
				K6Version:   consts.Version,
				Data:        []byte(`// a contents`),
				PwdURL:      &url.URL{Scheme: "file", Path: entry.Pwd},
				Filesystems: map[string]fsext.FS{
					"file": makeMemMapFs(t, map[string][]byte{
						fmt.Sprintf("%s/a.js", entry.Pwd):      []byte(`// a contents`),
						fmt.Sprintf("%s/b.js", entry.Pwd):      []byte(`// b contents`),
						fmt.Sprintf("%s/file1.txt", entry.Pwd): []byte(`hi!`),
						fmt.Sprintf("%s/file2.txt", entry.Pwd): []byte(`bye!`),
					}),
					"https": makeMemMapFs(t, map[string][]byte{
						"/cdnjs.com/libraries/Faker":          []byte(`// faker contents`),
						"/github.com/loadimpact/k6/README.md": []byte(`README`),
					}),
				},
			}
			arc1Anon := &Archive{
				Type: "js",
				Options: Options{
					VUs:        null.IntFrom(12345),
					SystemTags: &stats.DefaultSystemTagSet,
				},
				FilenameURL: &url.URL{Scheme: "file", Path: fmt.Sprintf("%s/a.js", entry.PwdNormAnon)},
				K6Version:   consts.Version,
				Data:        []byte(`// a contents`),
				PwdURL:      &url.URL{Scheme: "file", Path: entry.PwdNormAnon},

				Filesystems: map[string]fsext.FS{
					"file": makeMemMapFs(t, map[string][]byte{
						fmt.Sprintf("%s/a.js", entry.PwdNormAnon):      []byte(`// a contents`),
						fmt.Sprintf("%s/b.js", entry.PwdNormAnon):      []byte(`// b contents`),
						fmt.Sprintf("%s/file1.txt", entry.PwdNormAnon): []byte(`hi!`),
						fmt.Sprintf("%s/file2.txt", entry.PwdNormAnon): []byte(`bye!`),
					}),
					"https": makeMemMapFs(t, map[string][]byte{
						"/cdnjs.com/libraries/Faker":          []byte(`// faker contents`),
						"/github.com/loadimpact/k6/README.md": []byte(`README`),
					}),
				},
			}

			buf := bytes.NewBuffer(nil)
			require.NoError(t, arc1.Write(buf))

			arc1Filesystems := arc1Anon.Filesystems
			arc1Anon.Filesystems = nil

			arc2, err := ReadArchive(buf)
			assert.NoError(t, err)
			arc2.Filename = ""
			arc2.Pwd = ""

			arc2Filesystems := arc2.Filesystems
			arc2.Filesystems = nil

			assert.Equal(t, arc1Anon, arc2)
			diffMapFilesystems(t, arc1Filesystems, arc2Filesystems)
		}
	})
}

func TestArchiveJSONEscape(t *testing.T) {
	t.Parallel()

	arc := &Archive{}
	arc.Filename = "test<.js"
	b, err := arc.json()
	assert.NoError(t, err)
	assert.Contains(t, string(b), "test<.js")
}

func TestUsingCacheFromCacheOnReadFs(t *testing.T) {
	base := afero.NewMemMapFs()
	cached := afero.NewMemMapFs()
	// we specifically have different contents in both places
	require.NoError(t, afero.WriteFile(base, "/wrong", []byte(`ooops`), 0o644))
	require.NoError(t, afero.WriteFile(cached, "/correct", []byte(`test`), 0o644))

	arc := &Archive{
		Type:        "js",
		FilenameURL: &url.URL{Scheme: "file", Path: "/correct"},
		K6Version:   consts.Version,
		Data:        []byte(`test`),
		PwdURL:      &url.URL{Scheme: "file", Path: "/"},
		Filesystems: map[string]fsext.FS{
			"file": fsext.NewFS(fsext.NewCacheOnReadFs(base, cached, 0)),
		},
	}

	buf := bytes.NewBuffer(nil)
	require.NoError(t, arc.Write(buf))

	newArc, err := ReadArchive(buf)
	require.NoError(t, err)

	data, err := newArc.Filesystems["file"].ReadFile("/correct")
	require.NoError(t, err)
	require.Equal(t, string(data), "test")

	data, err = newArc.Filesystems["file"].ReadFile("/wrong")
	require.Error(t, err)
	require.Nil(t, data)
}

func TestArchiveWithDataNotInFS(t *testing.T) {
	t.Parallel()

	arc := &Archive{
		Type:        "js",
		FilenameURL: &url.URL{Scheme: "file", Path: "/script"},
		K6Version:   consts.Version,
		Data:        []byte(`test`),
		PwdURL:      &url.URL{Scheme: "file", Path: "/"},
		Filesystems: nil,
	}

	buf := bytes.NewBuffer(nil)
	err := arc.Write(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "the main script wasn't present in the cached filesystem")
}

func TestMalformedMetadata(t *testing.T) {
	fs := fsext.NewInMemoryFS()

	require.NoError(t, fs.WriteFile("/metadata.json", []byte("{,}"), 0o644))
	b, err := dumpMemMapFsToBuf(fs)
	require.NoError(t, err)
	_, err = ReadArchive(b)
	require.Error(t, err)
	require.Equal(t, err.Error(), `invalid character ',' looking for beginning of object key string`)
}

func TestStrangePaths(t *testing.T) {
	pathsToChange := []string{
		`/path/with spaces/a.js`,
		`/path/with spaces/a.js`,
		`/path/with日本語/b.js`,
		`/path/with spaces and 日本語/file1.txt`,
	}
	for _, pathToChange := range pathsToChange {
		otherMap := make(map[string][]byte, len(pathsToChange))
		for _, other := range pathsToChange {
			otherMap[other] = []byte(`// ` + other + ` contents`)
		}
		arc1 := &Archive{
			Type:      "js",
			K6Version: consts.Version,
			Options: Options{
				VUs:        null.IntFrom(12345),
				SystemTags: &stats.DefaultSystemTagSet,
			},
			FilenameURL: &url.URL{Scheme: "file", Path: pathToChange},
			Data:        []byte(`// ` + pathToChange + ` contents`),
			PwdURL:      &url.URL{Scheme: "file", Path: path.Dir(pathToChange)},
			Filesystems: map[string]fsext.FS{
				"file": makeMemMapFs(t, otherMap),
			},
		}

		buf := bytes.NewBuffer(nil)
		require.NoError(t, arc1.Write(buf), pathToChange)

		arc1Filesystems := arc1.Filesystems
		arc1.Filesystems = nil

		arc2, err := ReadArchive(buf)
		require.NoError(t, err, pathToChange)

		arc2Filesystems := arc2.Filesystems
		arc2.Filesystems = nil
		arc2.Filename = ""
		arc2.Pwd = ""

		assert.Equal(t, arc1, arc2, pathToChange)

		arc1Filesystems["https"] = fsext.NewInMemoryFS()
		diffMapFilesystems(t, arc1Filesystems, arc2Filesystems)
	}
}

func TestStdinArchive(t *testing.T) {
	inMemoryFS := fsext.NewInMemoryFS()
	// we specifically have different contents in both places
	require.NoError(t, inMemoryFS.WriteFile("/-", []byte(`test`), 0o644))

	arc := &Archive{
		Type:        "js",
		FilenameURL: &url.URL{Scheme: "file", Path: "/-"},
		K6Version:   consts.Version,
		Data:        []byte(`test`),
		PwdURL:      &url.URL{Scheme: "file", Path: "/"},
		Filesystems: map[string]fsext.FS{
			"file": inMemoryFS,
		},
	}

	buf := bytes.NewBuffer(nil)
	require.NoError(t, arc.Write(buf))

	newArc, err := ReadArchive(buf)
	require.NoError(t, err)

	data, err := afero.ReadFile(newArc.Filesystems["file"].Afero(), "/-")
	require.NoError(t, err)
	require.Equal(t, string(data), "test")
}
