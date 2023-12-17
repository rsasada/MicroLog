package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (errOffset ErrOffsetOutOfRange) GRPCStatus() *status.Status {

	st := status.New(
		codes.OutOfRange,
		fmt.Sprintf("offset out of range: %d", errOffset.Offset),
	)
	msg := fmt.Sprintf(
		"the requested offset is outside the log's range: %d",
		errOffset.Offset,
	)

	l_msg := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}
	std, err := st.WithDetails(l_msg)
	if err != nil {
		return st
	}
	return std
}

func (errOffset ErrOffsetOutOfRange) Error() string {

	return errOffset.GRPCStatus().Err().Error()
}
