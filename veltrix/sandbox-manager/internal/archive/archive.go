// internal/archive/archive.go — safe zip/tar extraction with strict size and path limits.
package archive

import (
	"archive/tar"
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Limits controls the safety bounds applied during extraction.
type Limits struct {
	MaxTotalBytes int64 // total uncompressed size allowed
	MaxFileSizeBytes int64 // per-file uncompressed size allowed
	MaxFileCount  int   // maximum number of files in the archive
}

// Extract detects the archive format and extracts safely into destDir.
// It rejects: absolute paths, path traversal (../), symlinks, and device files.
func Extract(archivePath, destDir string, limits Limits) error {
	// Try zip first (magic bytes: PK).
	if isZip(archivePath) {
		return extractZip(archivePath, destDir, limits)
	}
	// Fall back to tar (handles .tar, .tar.gz, .tar.bz2, etc.).
	return extractTar(archivePath, destDir, limits)
}

// ── Zip ───────────────────────────────────────────────────────────────────────

func isZip(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	magic := make([]byte, 4)
	if _, err := f.Read(magic); err != nil {
		return false
	}
	// ZIP local file header signature: 50 4B 03 04
	return magic[0] == 0x50 && magic[1] == 0x4B && magic[2] == 0x03 && magic[3] == 0x04
}

func extractZip(archivePath, destDir string, l Limits) error {
	zr, err := zip.OpenReader(archivePath)
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	defer zr.Close()

	var totalBytes int64
	fileCount := 0

	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		if err := validatePath(f.Name); err != nil {
			return err
		}
		// Reject symlinks stored in zip external attributes.
		if f.FileInfo().Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlinks not allowed in archive: %s", f.Name)
		}
		if f.UncompressedSize64 > uint64(l.MaxFileSizeBytes) {
			return fmt.Errorf("file %q exceeds per-file size limit (%d bytes)", f.Name, l.MaxFileSizeBytes)
		}
		totalBytes += int64(f.UncompressedSize64)
		if totalBytes > l.MaxTotalBytes {
			return fmt.Errorf("archive total uncompressed size exceeds limit (%d bytes)", l.MaxTotalBytes)
		}
		fileCount++
		if fileCount > l.MaxFileCount {
			return fmt.Errorf("archive contains too many files (limit: %d)", l.MaxFileCount)
		}

		destPath, err := safeJoin(destDir, f.Name)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
			return fmt.Errorf("mkdir for %q: %w", destPath, err)
		}

		src, err := f.Open()
		if err != nil {
			return fmt.Errorf("open zip entry %q: %w", f.Name, err)
		}
		if err := writeFile(destPath, src, l.MaxFileSizeBytes); err != nil {
			src.Close()
			return err
		}
		src.Close()
	}
	return nil
}

// ── Tar ───────────────────────────────────────────────────────────────────────

func extractTar(archivePath, destDir string, l Limits) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}
	defer f.Close()

	tr := tar.NewReader(f)
	var totalBytes int64
	fileCount := 0

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar: %w", err)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeReg, tar.TypeRegA:
			// OK — regular file
		default:
			return fmt.Errorf("unsupported tar entry type %d for %q", hdr.Typeflag, hdr.Name)
		}

		if err := validatePath(hdr.Name); err != nil {
			return err
		}
		if hdr.Size > l.MaxFileSizeBytes {
			return fmt.Errorf("file %q exceeds per-file size limit", hdr.Name)
		}
		totalBytes += hdr.Size
		if totalBytes > l.MaxTotalBytes {
			return fmt.Errorf("archive total size exceeds limit")
		}
		fileCount++
		if fileCount > l.MaxFileCount {
			return fmt.Errorf("too many files in archive (limit: %d)", l.MaxFileCount)
		}

		destPath, err := safeJoin(destDir, hdr.Name)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
			return fmt.Errorf("mkdir for %q: %w", destPath, err)
		}
		if err := writeFile(destPath, tr, l.MaxFileSizeBytes); err != nil {
			return err
		}
	}
	return nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func validatePath(name string) error {
	if filepath.IsAbs(name) {
		return fmt.Errorf("absolute path not allowed: %q", name)
	}
	cleaned := filepath.Clean(name)
	if strings.HasPrefix(cleaned, "..") {
		return fmt.Errorf("path traversal not allowed: %q", name)
	}
	return nil
}

func safeJoin(base, name string) (string, error) {
	dest := filepath.Join(base, filepath.Clean(name))
	absBase, err := filepath.Abs(base)
	if err != nil {
		return "", err
	}
	absDest, err := filepath.Abs(dest)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(absDest, absBase+string(os.PathSeparator)) {
		return "", fmt.Errorf("path escapes extraction directory: %q", name)
	}
	return dest, nil
}

func writeFile(destPath string, r io.Reader, maxBytes int64) error {
	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("create %q: %w", destPath, err)
	}
	defer out.Close()

	// LimitReader prevents zip-bomb decompression from writing more than the limit.
	if _, err := io.Copy(out, io.LimitReader(r, maxBytes+1)); err != nil {
		return fmt.Errorf("write %q: %w", destPath, err)
	}
	return nil
}
