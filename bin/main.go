package main

import (
	"context"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func Handler(ctx context.Context, req *events.APIGatewayProxyRequest) (*events.APIGatewayProxyResponse, error) {

	response := events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Body:       "success!",
	}

	return &response, nil
}

func main() {
	lambda.Start(Handler)
}
