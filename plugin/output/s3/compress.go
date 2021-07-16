package s3

import (
	"archive/zip"
	"fmt"
	"io"
	"os"

	"github.com/minio/minio-go"
	"go.uber.org/zap"
)

const (
	zipName = "zip"
)

type zipCompressor struct {
	logger  *zap.SugaredLogger
	options minio.PutObjectOptions
}

func newZipCompressor(logger *zap.SugaredLogger) compressor {
	z := zipCompressor{logger: logger}
	z.options = z.getObjectOptions()
	return &z
}

func (z *zipCompressor) getName(fileName string) string {
	return fmt.Sprintf("%s.%s", fileName, zipName)
}

func (z *zipCompressor) compress(archiveName, fileName string) {
	newZipFile, err := os.Create(archiveName)
	if err != nil {
		z.logger.Panicf("could not create zip file: %s, error: %s", archiveName, err.Error())
	}
	defer newZipFile.Close()
	zipWriter := zip.NewWriter(newZipFile)
	defer zipWriter.Close()

	fileToZip, err := os.Open(fileName)
	if err != nil {
		z.logger.Panicf("could not open file: %s, error: %s", fileName, err.Error())
	}
	defer fileToZip.Close()
	info, err := fileToZip.Stat()
	if err != nil {
		z.logger.Panicf("could not get file inforamtion for file: %s, error: %s", fileName, err.Error())
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		z.logger.Panicf("could not set file info header for file: %s, error: %s", fileName, err.Error())
	}
	header.Name = fileName

	// Change to deflate to gain better compression
	// see http://golang.org/pkg/archive/zip/#pkg-constants
	header.Method = zip.Deflate

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		z.logger.Panicf("could not create header for file: %s, error: %s", fileName, err.Error())
	}
	_, err = io.Copy(writer, fileToZip)
	if err != nil {
		z.logger.Panicf("could not add file: %s to archive, error: %s", fileName, err.Error())
	}
}

func (z *zipCompressor) getObjectOptions() minio.PutObjectOptions {
	return minio.PutObjectOptions{
		ContentType: "application/zip",
	}
}

func (z *zipCompressor) getExtension() string {
	return fmt.Sprintf(".%s", zipName)
}
