package apns

import "sync"

type pnQueue struct {
	sync.Mutex
	data []*PushNotification
	cap  int
}

func newPnQueue(cap int) *pnQueue {
	return &pnQueue{cap: cap, data: make([]*PushNotification, 0)}
}

func (p *pnQueue) Append(pn *PushNotification) {
	p.Lock()
	defer p.Unlock()

		p.data = append(p.data, pn)
	if len(p.data) > p.cap {
		p.data = p.data[len(p.data)-p.cap:]
	}
}

//find all PushNotifications after errPn
func (p *pnQueue) Tail(errId int32) (*PushNotification, []*PushNotification) {
	p.Lock()
	defer p.Unlock()

	for i, pn := range p.data {
		if pn.Identifier == errId {
			if (i + 1 < len(p.data)){
				return pn, p.data[i + 1:]
			}else{
				return pn, nil
			}
		}
	}
	return nil, nil
}

func (p *pnQueue) Clear(){
	p.Lock()
	defer p.Unlock()

	p.data = []*PushNotification{}
}
