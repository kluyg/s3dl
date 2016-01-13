package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var awsRegion = flag.String("region", "us-west-1", "AWS Region")
var bucket = flag.String("bucket", "", "S3 bucket (required)")
var prefix = flag.String("prefix", "", "S3 key prefix (required)")

func main() {
	flag.Parse()
	argsErr := "Missing required arg:"
	if *bucket == "" {
		log.Fatalln(argsErr, "bucket")
	}
	if *prefix == "" {
		log.Fatalln(argsErr, "prefix")
	}

	awsSession := session.New(&aws.Config{Region: aws.String(*awsRegion)})
	svc := s3.New(awsSession)
	loi := &s3.ListObjectsInput{
		Bucket: bucket,
		Prefix: prefix,
	}
	listObjectsO, err := svc.ListObjects(loi)
	if err != nil {
		log.Fatalln("ListObjects failed:", err)
	}
	if *listObjectsO.IsTruncated {
		fmt.Println("ListObjectsOutput is truncated")
	}
	downloader := s3manager.NewDownloader(awsSession)
	fmt.Println("found keys:")
	for _, v := range listObjectsO.Contents {
		key := *v.Key
		fmt.Print("Downloading ", key, " ... ")
		file := getFile(key)
		_, err := downloader.Download(file,
			&s3.GetObjectInput{
				Bucket: aws.String(*bucket),
				Key:    aws.String(key),
			})
		if err != nil {
			log.Fatalln("Failed to download file", err)
		}
		fmt.Println("done")
		file.Close()
	}
	fmt.Println("DONE")
}

func getFile(key string) *os.File {
	fields := strings.Split(key, "/")
	filename := fields[len(fields)-1]
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalln("Failed to create file", err)
	}
	return file
}
