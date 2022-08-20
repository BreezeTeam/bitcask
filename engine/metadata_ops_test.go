package bitcask

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteMetadataAtomicallyFaultStages(t *testing.T) {
	for _, stage := range []metadataStage{
		metadataStageTempWrite,
		metadataStageFileSync,
		metadataStageRename,
		metadataStageDirSync,
	} {
		t.Run(string(stage), func(t *testing.T) {
			dir := t.TempDir()
			tempPath := filepath.Join(dir, "state.tmp")
			finalPath := filepath.Join(dir, "state")
			if err := os.WriteFile(finalPath, []byte("old"), 0644); err != nil {
				t.Fatal(err)
			}
			opt := DefaultOptions
			opt.FaultInjection.Enable = true
			opt.FaultInjection.MetadataStage = string(stage)
			opt.FaultInjection.MetadataFailAfter = 0
			opt.faultState = &faultInjectionState{}
			if err := writeMetadataAtomically(opt, tempPath, finalPath, []byte("new"), 0644); !errors.Is(err, ErrFaultInjectedMetadata) {
				t.Fatalf("got %v want %v", err, ErrFaultInjectedMetadata)
			}
			got, err := os.ReadFile(finalPath)
			if err != nil {
				t.Fatal(err)
			}
			want := "old"
			if stage == metadataStageDirSync {
				want = "new"
			}
			if string(got) != want {
				t.Fatalf("final bytes %q want %q", got, want)
			}

			opt.FaultInjection.Enable = false
			if err := writeMetadataAtomically(opt, tempPath, finalPath, []byte("retry"), 0644); err != nil {
				t.Fatal(err)
			}
			got, err = os.ReadFile(finalPath)
			if err != nil || string(got) != "retry" {
				t.Fatalf("retry bytes %q err=%v", got, err)
			}
		})
	}
}

func TestMetadataStageCountersAreIsolated(t *testing.T) {
	opt := DefaultOptions
	opt.FaultInjection.Enable = true
	opt.FaultInjection.MetadataStage = string(metadataStageManifestDeleteDirSync)
	opt.FaultInjection.MetadataFailAfter = 0
	opt.faultState = &faultInjectionState{}
	publicationCalled := false
	if err := runMetadataOperation(opt, metadataStageDirSync, func() error {
		publicationCalled = true
		return nil
	}); err != nil || !publicationCalled {
		t.Fatalf("publication operation err=%v called=%t", err, publicationCalled)
	}
	cleanupCalled := false
	if err := runMetadataOperation(opt, metadataStageManifestDeleteDirSync, func() error {
		cleanupCalled = true
		return nil
	}); !errors.Is(err, ErrFaultInjectedMetadata) || cleanupCalled {
		t.Fatalf("cleanup err=%v called=%t", err, cleanupCalled)
	}
}

func BenchmarkMetadataStageInjectionCheck(b *testing.B) {
	opt := DefaultOptions
	opt.FaultInjection.Enable = true
	opt.FaultInjection.MetadataStage = string(metadataStageManifestDeleteDirSync)
	opt.FaultInjection.MetadataFailAfter = -1
	opt.faultState = &faultInjectionState{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := runMetadataOperation(opt, metadataStageManifestDeleteDirSync, func() error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteMetadataAtomically(b *testing.B) {
	dir := b.TempDir()
	opt := DefaultOptions
	data := []byte("manifest-bytes")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := writeMetadataAtomically(
			opt,
			filepath.Join(dir, "state.tmp"),
			filepath.Join(dir, "state"),
			data,
			0644,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func TestWriteMetadataAtomicallyFaultOccurrence(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions
	opt.FaultInjection.Enable = true
	opt.FaultInjection.MetadataStage = string(metadataStageRename)
	opt.FaultInjection.MetadataFailAfter = 1
	opt.faultState = &faultInjectionState{}
	for attempt := 0; attempt < 2; attempt++ {
		err := writeMetadataAtomically(
			opt,
			filepath.Join(dir, "state.tmp"),
			filepath.Join(dir, "state"),
			[]byte{byte(attempt)},
			0644,
		)
		if attempt == 0 && err != nil {
			t.Fatalf("first attempt: %v", err)
		}
		if attempt == 1 && !errors.Is(err, ErrFaultInjectedMetadata) {
			t.Fatalf("second attempt got %v want %v", err, ErrFaultInjectedMetadata)
		}
	}
}
