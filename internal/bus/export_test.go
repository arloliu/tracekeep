package bus

// SetHookAfterInfo sets the seam between the stream's first Info and the catalog's read in Incarnate.
func SetHookAfterInfo(h func()) { hookAfterInfo = h }
