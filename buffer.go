package audriver

type buffer struct {
	ms []DatabaseModification
}

func (b *buffer) add(op DatabaseModification) {
	b.ms = append(b.ms, op)
}

func (b *buffer) drain() []DatabaseModification {
	if len(b.ms) == 0 {
		return nil
	}

	ms := b.ms
	b.ms = nil
	return ms
}
