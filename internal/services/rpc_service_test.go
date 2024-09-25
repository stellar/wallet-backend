package services

// type errorReader struct{}

// func (e *errorReader) Read(p []byte) (n int, err error) {
// 	return 0, fmt.Errorf("read error")
// }

// func (e *errorReader) Close() error {
// 	return nil
// }

// func TestSendRPCRequest(t *testing.T) {
// 	mockHTTPClient := utils.MockHTTPClient{}
// 	rpcURL := "http://localhost:8000/soroban/rpc"
// 	rpcService := NewRPCService(rpcURL, &mockHTTPClient)
// 	method := "sendTransaction"
// 	params := map[string]string{"transaction": "ABCD"}
// 	payload := map[string]interface{}{
// 		"jsonrpc": "2.0",
// 		"id":      1,
// 		"method":  method,
// 		"params":  params,
// 	}
// 	jsonData, _ := json.Marshal(payload)
// 	t.Run("rpc_post_call_fails", func(t *testing.T) {
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(&http.Response{}, errors.New("RPC Connection fail")).
// 			Once()

// 		resp, err := rpcService.sendRPCRequest(method, params)

// 		assert.Empty(t, resp)
// 		assert.Equal(t, "sendTransaction: sending POST request to rpc: RPC Connection fail", err.Error())
// 	})

// 	t.Run("unmarshaling_rpc_response_fails", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(&errorReader{}),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.sendRPCRequest(method, params)

// 		assert.Empty(t, resp)
// 		assert.Equal(t, "sendTransaction: unmarshaling RPC response", err.Error())
// 	})

// 	t.Run("unmarshaling_json_fails", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(strings.NewReader(`{invalid-json`)),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.sendRPCRequest(method, params)

// 		assert.Empty(t, resp)
// 		assert.Equal(t, "sendTransaction: parsing RPC response JSON", err.Error())
// 	})

// 	t.Run("response_has_no_result_field", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(strings.NewReader(`{"status": "success"}`)),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.sendRPCRequest(method, params)

// 		assert.Empty(t, resp)
// 		assert.Equal(t, "sendTransaction: response missing result field", err.Error())
// 	})

// 	t.Run("response_has_status_field", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(strings.NewReader(`{"result": {"status": "PENDING"}}`)),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.sendRPCRequest(method, params)

// 		assert.Equal(t, "PENDING", resp.Result.Status)
// 		assert.Empty(t, err)
// 	})

// 	t.Run("response_has_envelopexdr_field", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(strings.NewReader(`{"result": {"envelopeXdr": "exdr"}}`)),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.sendRPCRequest(method, params)

// 		assert.Equal(t, "exdr", resp.Result.EnvelopeXDR)
// 		assert.Empty(t, err)
// 	})

// 	t.Run("response_has_resultxdr_field", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(strings.NewReader(`{"result": {"resultXdr": "rxdr"}}`)),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.sendRPCRequest(method, params)

// 		assert.Equal(t, "rxdr", resp.Result.ResultXDR)
// 		assert.Empty(t, err)
// 	})

// 	t.Run("response_has_errorresultxdr_field", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(strings.NewReader(`{"result": {"errorResultXdr": "exdr"}}`)),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.sendRPCRequest(method, params)

// 		assert.Equal(t, "exdr", resp.Result.ErrorResultXDR)
// 		assert.Empty(t, err)
// 	})

// 	t.Run("response_has_hash_field", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(strings.NewReader(`{"result": {"hash": "hash"}}`)),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.sendRPCRequest(method, params)

// 		assert.Equal(t, "hash", resp.Result.Hash)
// 		assert.Empty(t, err)
// 	})

// 	t.Run("response_has_createdat_field", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(strings.NewReader(`{"result": {"createdAt": "1234"}}`)),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.sendRPCRequest(method, params)

// 		assert.Equal(t, "1234", resp.Result.CreatedAt)
// 		assert.Empty(t, err)
// 	})
// }

// func TestSendTransaction(t *testing.T) {
// 	mockHTTPClient := utils.MockHTTPClient{}
// 	rpcURL := "http://localhost:8000/soroban/rpc"
// 	rpcService := NewRPCService(rpcURL, &mockHTTPClient)
// 	method := "sendTransaction"
// 	params := map[string]string{"transaction": "ABCD"}
// 	payload := map[string]interface{}{
// 		"jsonrpc": "2.0",
// 		"id":      1,
// 		"method":  method,
// 		"params":  params,
// 	}
// 	jsonData, _ := json.Marshal(payload)

// 	t.Run("rpc_request_fails", func(t *testing.T) {
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(&http.Response{}, errors.New("RPC Connection fail")).
// 			Once()

// 		resp, err := rpcService.SendTransaction("ABCD")

// 		assert.Equal(t, tss.RPCFailCode, resp.Code.OtherCodes)
// 		assert.Equal(t, "RPC fail: sendTransaction: sending POST request to rpc: RPC Connection fail", err.Error())

// 	})
// 	t.Run("response_has_empty_errorResultXdr", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(strings.NewReader(`{"result": {"status": "PENDING", "errorResultXdr": ""}}`)),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.SendTransaction("ABCD")

// 		assert.Equal(t, entities.PendingStatus, resp.Status)
// 		assert.Equal(t, tss.UnmarshalBinaryCode, resp.Code.OtherCodes)
// 		assert.Equal(t, "parse error result xdr string: unable to unmarshal errorResultXdr: ", err.Error())

// 	})
// 	t.Run("response_has_unparsable_errorResultXdr", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(strings.NewReader(`{"result": {"errorResultXdr": "ABC123"}}`)),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.SendTransaction("ABCD")

// 		assert.Equal(t, tss.UnmarshalBinaryCode, resp.Code.OtherCodes)
// 		assert.Equal(t, "parse error result xdr string: unable to unmarshal errorResultXdr: ABC123", err.Error())
// 	})
// 	t.Run("response_has_errorResultXdr", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(strings.NewReader(`{"result": {"errorResultXdr": "AAAAAAAAAMj////9AAAAAA=="}}`)),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.SendTransaction("ABCD")

// 		assert.Equal(t, xdr.TransactionResultCodeTxTooLate, resp.Code.TxResultCode)
// 		assert.Empty(t, err)
// 	})
// }

// func TestGetTransaction(t *testing.T) {
// 	mockHTTPClient := utils.MockHTTPClient{}
// 	rpcURL := "http://localhost:8000/soroban/rpc"
// 	rpcService := NewRPCService(rpcURL, &mockHTTPClient)
// 	method := "getTransaction"
// 	params := map[string]string{"hash": "XYZ"}
// 	payload := map[string]interface{}{
// 		"jsonrpc": "2.0",
// 		"id":      1,
// 		"method":  method,
// 		"params":  params,
// 	}
// 	jsonData, _ := json.Marshal(payload)

// 	t.Run("rpc_request_fails", func(t *testing.T) {
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(&http.Response{}, errors.New("RPC Connection fail")).
// 			Once()

// 		resp, err := rpcService.GetTransaction("XYZ")

// 		assert.Equal(t, entities.ErrorStatus, resp.Status)
// 		assert.Equal(t, "RPC Fail: getTransaction: sending POST request to rpc: RPC Connection fail", err.Error())

// 	})
// 	t.Run("unable_to_parse_createdAt", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(strings.NewReader(`{"result": {"status": "SUCCESS", "createdAt": "ABCD"}}`)),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.GetTransaction("XYZ")

// 		assert.Equal(t, entities.ErrorStatus, resp.Status)
// 		assert.Equal(t, "unable to parse createAt: strconv.ParseInt: parsing \"ABCD\": invalid syntax", err.Error())
// 	})
// 	t.Run("response_has_createdAt_field", func(t *testing.T) {
// 		httpResponse := &http.Response{
// 			StatusCode: http.StatusOK,
// 			Body:       io.NopCloser(strings.NewReader(`{"result": {"createdAt": "1234567"}}`)),
// 		}
// 		mockHTTPClient.
// 			On("Post", rpcURL, "application/json", bytes.NewBuffer(jsonData)).
// 			Return(httpResponse, nil).
// 			Once()

// 		resp, err := rpcService.GetTransaction("XYZ")

// 		assert.Equal(t, int64(1234567), resp.CreatedAt)
// 		assert.Empty(t, err)
// 	})
// }
