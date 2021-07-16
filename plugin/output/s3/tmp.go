package s3

import (
	"fmt"
	"os"
	"time"

	"github.com/minio/minio-go"
	"github.com/ozonru/file.d/logger"
)


type mockClient struct {

}

func NewMockClient () objectStoreClient{
	return mockClient{}
}
func (m mockClient) BucketExists(bucketName string) (bool, error) {
	return true, nil

}


func (m mockClient) FPutObject(bucketName, objectName, filePath string, opts minio.PutObjectOptions) (n int64, err error) {
	targetDir := fmt.Sprintf("./%s", bucketName)
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		if err := os.MkdirAll(targetDir, os.ModePerm); err != nil {
			logger.Fatalf("could not create target dir: %s, error: %s", targetDir, err.Error())
		}
	}
	fileName := fmt.Sprintf("%s/%s", bucketName, "mockLog.txt" )
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(0777))
	if err != nil {
		logger.Panicf("could not open or create file: %s, error: %s", fileName, err.Error())
	}

	if _, err := file.WriteString(fmt.Sprintf("%s | from '%s' to b: `%s` as obj: `%s`\n", time.Now().String(), filePath, bucketName, objectName)); err != nil {
		return 0, fmt.Errorf(err.Error())
	}

	return 1, nil
}
