package functions

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Item represents a record in DynamoDB
type Item struct {
	Hash      string `json:"hash"`
	Filename  string `json:"filename"`
	Timestamp string `json:"timestamp"`
}

// GetLatestHashFilePairAndZip returns the latest hash and filename pair from DynamoDB and zips all files from S3
func GetLatestHashFilePairAndZip(req *events.APIGatewayProxyRequest) (*events.APIGatewayProxyResponse, error) {
	// Get bucket name from environment variable
	bucketName := os.Getenv("bucketName")
	if bucketName == "" {
		errMessage := "S3 bucket name is not set"
		fmt.Println(errMessage)
		return &events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       errMessage,
		}, nil
	}

	// Fetch the latest hash value from DynamoDB
	hash, _, err := GetLatestHashFilePair()
	if err != nil {
		fmt.Printf("Error fetching latest hash from DynamoDB: %s\n", err.Error())
		return &events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Error fetching latest hash from DynamoDB: " + err.Error(),
		}, nil
	}

	// Initialize a buffer to store the zip file
	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)

	// Get all objects recursively from S3 and add them to the zip
	err = fetchAndZipObjects(bucketName, "", zipWriter)
	if err != nil {
		fmt.Printf("Error fetching and zipping objects: %s\n", err.Error())
		return &events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Error fetching and zipping objects: " + err.Error(),
		}, nil
	}

	// Close the zip writer
	err = zipWriter.Close()
	if err != nil {
		fmt.Printf("Error closing zip writer: %s\n", err.Error())
		return &events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Error closing zip writer: " + err.Error(),
		}, nil
	}

	// Encode the zip file content as base64
	encodedZip := base64.StdEncoding.EncodeToString(buf.Bytes())

	// Set the filename using the hash value
	filename := hash + ".zip"

	// Return the base64-encoded zip file content in response
	return &events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Body:       encodedZip,
		Headers: map[string]string{
			"Content-Type":        "application/zip",
			"Content-Disposition": fmt.Sprintf("attachment; filename=\"%s\"", filename),
		},
	}, nil
}

// GetLatestHashFilePair gets the latest hash and filename pair from DynamoDB
func GetLatestHashFilePair() (string, string, error) {
	// Create a new AWS session with default configuration
	sess, err := session.NewSession()
	if err != nil {
		return "", "", fmt.Errorf("failed to create AWS session: %w", err)
	}

	// Create a DynamoDB service client
	svc := dynamodb.New(sess)

	// Prepare the input parameters for the Scan request
	input := &dynamodb.ScanInput{
		TableName: aws.String("file-script"),
	}

	// Scan the table
	result, err := svc.Scan(input)
	if err != nil {
		return "", "", fmt.Errorf("failed to scan DynamoDB table: %w", err)
	}

	if len(result.Items) == 0 {
		return "", "", fmt.Errorf("no items found in DynamoDB table")
	}

	// Unmarshal the results into a slice of Item
	var items []Item
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &items)
	if err != nil {
		return "", "", fmt.Errorf("failed to unmarshal DynamoDB scan result: %w", err)
	}

	// Sort the items by timestamp in descending order
	sort.Slice(items, func(i, j int) bool {
		ti, _ := time.Parse(time.RFC3339, items[i].Timestamp)
		tj, _ := time.Parse(time.RFC3339, items[j].Timestamp)
		return ti.After(tj)
	})

	// Return the hash and filename of the latest item
	latestItem := items[0]
	return latestItem.Hash, latestItem.Filename, nil
}

// Fetch and zip all objects in S3 recursively
func fetchAndZipObjects(bucketName, prefix string, zipWriter *zip.Writer) error {
	// Create a new AWS session with default configuration
	sess, err := session.NewSession()
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %w", err)
	}

	// Create an S3 service client
	svc := s3.New(sess)

	// List objects in the bucket with the specified prefix
	listInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucketName),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(1000), // Adjust as per your requirements
	}

	listOutput, err := svc.ListObjectsV2(listInput)
	if err != nil {
		return fmt.Errorf("failed to list objects in S3 bucket: %w", err)
	}

	for _, obj := range listOutput.Contents {
		// Skip directories
		if strings.HasSuffix(*obj.Key, "/") {
			continue
		}

		// Extract the file name from the object key
		fileName := strings.TrimPrefix(*obj.Key, prefix)

		// Get the object from S3
		input := &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		}

		result, err := svc.GetObject(input)
		if err != nil {
			return fmt.Errorf("failed to get object %s from S3: %w", *obj.Key, err)
		}
		defer result.Body.Close()

		// Create a file in the zip writer with the relative path
		zipFile, err := zipWriter.Create(fileName)
		if err != nil {
			return fmt.Errorf("failed to create file in zip: %w", err)
		}

		// Copy object content to the zip file
		_, err = io.Copy(zipFile, result.Body)
		if err != nil {
			return fmt.Errorf("failed to copy object content to zip: %w", err)
		}
	}

	return nil
}
