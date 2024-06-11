package handler

import (
	"go_lambdas/pkg/functions"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
)

func Handler(req *events.APIGatewayProxyRequest) (*events.APIGatewayProxyResponse, error) {
	switch req.HTTPMethod {
	case "GET":
		return functions.GetLatestHashFilePairAndZip(req)
	default:
		return &events.APIGatewayProxyResponse{
			StatusCode: http.StatusMethodNotAllowed,
			Body:       "Method not allowed",
		}, nil
	}
}
