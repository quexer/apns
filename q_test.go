package apns

import "testing"

func TestQ(t *testing.T){
	q := newPnQueue(1000)
	for i  := 0; i < 1000; i ++ {
		pn :=NewPushNotification()
		pn.Identifier = int32(i)
		q.Append(pn)
	}
	p, l := q.Tail(499)

	if p == nil || len(l) != 500{
		t.Error("tail err", p, len(l))
	}

	pn := NewPushNotification()
	pn.Identifier = 10001
	q.Append(pn)

	p, l = q.Tail(500)
	if p == nil || len(l) != 500{
		t.Error("tail after shift err")
	}

	if p.Identifier != 500{
		t.Error("shift err", p.Identifier)
	}

	p, l = q.Tail(80000)
	if p != nil || len(l) != 0 {
		t.Error("tail for unkown, err")
	}

	q.Clear()

	p, _ = q.Tail(999)
	if p != nil {
		t.Error("clear err")
	}
}
