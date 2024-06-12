package functions

import (
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

// GetLatestHashFilePairAndZip returns the latest hash and filename pair from DynamoDB and fetches the zip file from S3
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

	// Construct the path to the zip file in the S3 bucket
	zipFilePath := hash + "/"
	zipFileName, err := getZipFileFromFolder(bucketName, zipFilePath)
	if err != nil {
		fmt.Printf("Error fetching zip file from S3: %s\n", err.Error())
		return &events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Error fetching zip file from S3: " + err.Error(),
		}, nil
	}

	// Get the zip file from S3
	sess, err := session.NewSession()
	if err != nil {
		fmt.Printf("Failed to create AWS session: %s\n", err.Error())
		return &events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Failed to create AWS session: " + err.Error(),
		}, nil
	}

	svc := s3.New(sess)
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(zipFilePath + zipFileName),
	}

	result, err := svc.GetObject(input)
	if err != nil {
		fmt.Printf("Failed to get object %s from S3: %s\n", zipFilePath+zipFileName, err.Error())
		return &events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Failed to get object from S3: " + err.Error(),
		}, nil
	}
	defer result.Body.Close()

	// Read the zip file content
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, result.Body)
	if err != nil {
		fmt.Printf("Failed to read object content: %s\n", err.Error())
		return &events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Failed to read object content: " + err.Error(),
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

// getZipFileFromFolder fetches the zip file from the specified S3 folder
func getZipFileFromFolder(bucketName, folderPath string) (string, error) {
	// Create a new AWS session with default configuration
	sess, err := session.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create AWS session: %w", err)
	}

	// Create an S3 service client
	svc := s3.New(sess)

	// List objects in the folder
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(folderPath),
	}

	listOutput, err := svc.ListObjectsV2(listInput)
	if err != nil {
		return "", fmt.Errorf("failed to list objects in S3 folder: %w", err)
	}

	// Find the zip file in the folder
	for _, obj := range listOutput.Contents {
		if strings.HasSuffix(*obj.Key, ".zip") {
			return strings.TrimPrefix(*obj.Key, folderPath), nil
		}
	}

	return "", fmt.Errorf("no zip file found in folder: %s", folderPath)
}
