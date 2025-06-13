package audriver

import (
	"database/sql/driver"
)

type Option func(*Driver)

// WithIDGenerator sets the ID generator for database modifications.
func WithIDGenerator(gen IDGenerator) Option {
	return func(d *Driver) {
		d.builder.idGenerator = gen
	}
}

// WithOperatorIDExtractor sets the operator ID extractor for database modifications.
func WithOperatorIDExtractor(extractor OperatorIDExtractor) Option {
	return func(d *Driver) {
		d.builder.operatorIDExtractor = extractor
	}
}

// WithExecutionIDExtractor sets the execution ID extractor for database modifications.
func WithExecutionIDExtractor(extractor ExecutionIDExtractor) Option {
	return func(d *Driver) {
		d.builder.executionIDExtractor = extractor
	}
}

func WithTableFilters(filters ...TableFilter) Option {
	return func(d *Driver) {
		d.builder.tableFilters = filters
	}
}

// Driver is a wrapper around a standard SQL driver that logs database modifications.
// It implements the driver.Driver interface and provides additional functionality for auditing.
type Driver struct {
	driver.Driver
	builder *databaseModificationBuilder
}

func New(driver driver.Driver, options ...Option) *Driver {
	drv := &Driver{
		Driver:  driver,
		builder: &databaseModificationBuilder{},
	}

	for _, option := range options {
		option(drv)
	}

	drv.builder.fillDefaults()

	return drv
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return &auditConn{Conn: conn, builder: d.builder}, nil
}

var (
	_ driver.Driver = (*Driver)(nil)
)
