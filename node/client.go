package node

import "context"

type clientRequestCall struct {
	command []byte
	reply   chan clientRequestResult
}

type clientRequestResult struct {
	success    bool
	leaderHint PeerID
	err        error
}

func (n *Node) submitClientRequest(
	ctx context.Context,
	command []byte,
) (clientRequestResult, error) {
	replyCh := make(chan clientRequestResult, 1)

	call := clientRequestCall{
		command: append([]byte(nil), command...),
		reply:   replyCh,
	}

	select {
	case n.clientRequestCh <- call:
	case <-ctx.Done():
		return clientRequestResult{}, ctx.Err()
	}

	select {
	case result := <-replyCh:
		return result, result.err
	case <-ctx.Done():
		return clientRequestResult{}, ctx.Err()
	}
}
