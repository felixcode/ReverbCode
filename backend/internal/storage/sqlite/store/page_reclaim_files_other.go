//go:build !linux

package store

func observeReclaimFiles(string, uint64, uint64) PageReclaimFiles { return PageReclaimFiles{} }
