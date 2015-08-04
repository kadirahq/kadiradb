package main

type point struct {
	value float64
	count uint32
}

func (p *point) add(q *point) {
	p.value += q.value
	p.count += q.count
}

func (p *point) toResult() (item *ResPoint) {
	item = &ResPoint{}
	item.Value = p.value
	item.Count = p.count
	return item
}

type series struct {
	fields []string
	points []*point
	data   [][]byte
}

func (sr *series) add(sn *series) {
	count := len(sr.points)
	for i := 0; i < count; i++ {
		sr.points[i].add(sn.points[i])
	}
}

func (sr *series) canMerge(sn *series) (can bool) {
	count := len(sr.fields)
	for i := 0; i < count; i++ {
		if sr.fields[i] != sn.fields[i] {
			return false
		}
	}

	return true
}

func (sr *series) toResult() (item *ResSeries) {
	item = &ResSeries{}
	item.Fields = sr.fields

	count := len(sr.points)
	item.Points = make([]*ResPoint, count, count)
	for i, p := range sr.points {
		point := p.toResult()
		item.Points[i] = point
	}

	return item
}

type seriesSet struct {
	items   []*series
	groupBy []bool
}

func (ss *seriesSet) add(sn *series) {
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

func (ss *seriesSet) grpFields(sn *series) {
	count := len(sn.fields)

	if grpCount := len(ss.groupBy); grpCount < count {
		count = grpCount
	}

	grouped := make([]string, count, count)

	for i := 0; i < count; i++ {
		if ss.groupBy[i] {
			grouped[i] = sn.fields[i]
		}
	}

	sn.fields = grouped
}

func (ss *seriesSet) toResult() (res []*ResSeries) {
	count := len(ss.items)
	res = make([]*ResSeries, count, count)

	for i := 0; i < count; i++ {
		sr := ss.items[i]
		item := sr.toResult()
		res[i] = item
	}

	return res
}
