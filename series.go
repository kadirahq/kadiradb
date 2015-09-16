package main

func newResPoint(val float64, num uint32) (p *ResPoint) {
	return &ResPoint{Value: val, Count: num}
}

func (p *ResPoint) add(q *ResPoint) {
	p.Value += q.Value
	p.Count += q.Count
}

func newResSeries(fields []string) (sr *ResSeries) {
	return &ResSeries{Fields: fields, Points: []*ResPoint{}}
}

func (sr *ResSeries) add(sn *ResSeries) {
	count := len(sr.Points)
	for i := 0; i < count; i++ {
		sr.Points[i].add(sn.Points[i])
	}
}

func (sr *ResSeries) canMerge(sn *ResSeries) (can bool) {
	count := len(sr.Fields)
	for i := 0; i < count; i++ {
		if sr.Fields[i] != sn.Fields[i] {
			return false
		}
	}

	return true
}

type seriesSet struct {
	items   []*ResSeries
	groupBy []bool
}

func (ss *seriesSet) add(sn *ResSeries) {
	ss.grpFields(sn)

	count := len(ss.items)
	for i := 0; i < count; i++ {
		sr := ss.items[i]
		if sr.canMerge(sn) {
			sr.add(sn)
			return
		}
	}

	ss.items = append(ss.items, sn)
}

func (ss *seriesSet) grpFields(sn *ResSeries) {
	count := len(sn.Fields)

	if grpCount := len(ss.groupBy); grpCount < count {
		count = grpCount
	}

	grouped := make([]string, count)
	for i := 0; i < count; i++ {
		if ss.groupBy[i] {
			grouped[i] = sn.Fields[i]
		}
	}

	sn.Fields = grouped
}

func (ss *seriesSet) toResult() (res []*ResSeries) {
	count := len(ss.items)
	res = make([]*ResSeries, count)

	for i := 0; i < count; i++ {
		sr := ss.items[i]
		res[i] = sr
	}

	return res
}
