package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestPutHandler(t *testing.T) {
	mux := http.NewServeMux()
	ts := httptest.NewServer(registerRoutes(mux, false))
	defer ts.Close()

	file, err := os.Open("put.txt")
	if err != nil {
		t.Error(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) != 3 {
			t.Fatalf("wrong format of test file")
		}
		method := string(parts[0])
		key := parts[1]
		expectedResult := parts[2]

		body := strings.NewReader(expectedResult) // NOTE: I give GET requests body as well, not needed tho

		url := fmt.Sprintf("%s/%s", ts.URL, key)
		req, _ := http.NewRequest(method, url, body)

		client := ts.Client()
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("something went wrong on request")
		}

		respBody, _ := io.ReadAll(resp.Body)

		if method == "GET" {
			if expectedResult == "NOT_FOUND" {
				if resp.StatusCode != http.StatusNotFound {
					t.Errorf("expected %d, got %d", http.StatusNotFound, resp.StatusCode)
				}
			} else {
				if string(respBody) != expectedResult {
					t.Errorf("expected %s, got %s", string(expectedResult), string(respBody))
				}
			}
		}
		if method == "PUT" {
			if resp.StatusCode != http.StatusOK {
				t.Errorf("expeced OK on put request and got %d", resp.StatusCode)
			}
		}

		resp.Body.Close()
	}

	if err := scanner.Err(); err != nil {
		t.Errorf("scanner had an error: %v", err)
	}
}
