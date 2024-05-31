package handler

import (
	"github.com/aws/aws-lambda-go/events"
)

// APIResponse represents the response structure for the APIGatewayProxyResponse
type APIResponse struct {
	StatusCode int
	Body       string
}

// receiver function: converting APIResponse objects to events.APIGatewayProxyResponse
func (resp *APIResponse) ToAPIGatewayProxyResponse() *events.APIGatewayProxyResponse {
	return &events.APIGatewayProxyResponse{
		StatusCode: resp.StatusCode,
		Body:       resp.Body,
	}
}
