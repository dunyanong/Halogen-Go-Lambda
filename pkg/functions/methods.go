package functions

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
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

// ReadZipFileFromS3 reads the ZIP file from the specified S3 bucket and file name
func ReadZipFileFromS3(bucketName, fileName string) ([]byte, error) {
	// Create a new AWS session with default configuration
	sess, err := session.NewSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}

	// Create an S3 service client
	svc := s3.New(sess)

	// Prepare the input parameters for the GetObject request
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fileName),
	}

	// Get the object from S3
	result, err := svc.GetObject(input)
	if err != nil {
		return nil, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	// Read the object body
	body, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body: %w", err)
	}

	return body, nil
}

// GetLatestFileKeyFromS3 lists objects in the bucket and returns the key of the latest file
func GetLatestFileKeyFromS3(bucketName string) (string, error) {
	// Create a new AWS session with default configuration
	sess, err := session.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create AWS session: %w", err)
	}

	// Create an S3 service client
	svc := s3.New(sess)

	// List the objects in the bucket
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	listOutput, err := svc.ListObjectsV2(listInput)
	if err != nil {
		return "", fmt.Errorf("failed to list objects in S3 bucket: %w", err)
	}

	// Sort the objects by LastModified timestamp in descending order
	sort.Slice(listOutput.Contents, func(i, j int) bool {
		return listOutput.Contents[i].LastModified.After(*listOutput.Contents[j].LastModified)
	})

	// Return the key of the latest object
	if len(listOutput.Contents) == 0 {
		return "", fmt.Errorf("no objects found in bucket")
	}

	return *listOutput.Contents[0].Key, nil
}

// GetZipFileFromS3 handles the API Gateway request and returns the ZIP file content from S3
func GetZipFileFromS3(req *events.APIGatewayProxyRequest) (*events.APIGatewayProxyResponse, error) {
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

	// Get the latest file key from the S3 bucket
	fileKey, err := GetLatestFileKeyFromS3(bucketName)
	if err != nil {
		errMessage := fmt.Sprintf("Error getting the latest file from S3: %s", err.Error())
		fmt.Println(errMessage)
		return &events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       errMessage,
		}, nil
	}

	fmt.Printf("Fetching latest file from S3. Bucket: %s, File: %s\n", bucketName, fileKey)

	// Read the zip file from S3
	data, err := ReadZipFileFromS3(bucketName, fileKey)
	if err != nil {
		fmt.Printf("Error reading file from S3: %s\n", err.Error())
		return &events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Error reading file from S3: " + err.Error(),
		}, nil
	}

	// Return the zip file content in response
	return &events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Body:       string(data),
		Headers: map[string]string{
			"Content-Type":        "application/zip",
			"Content-Disposition": fmt.Sprintf("attachment; filename=\"%s\"", fileKey),
		},
		IsBase64Encoded: true,
	}, nil
}
