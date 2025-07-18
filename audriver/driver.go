package audriver

import (
	"database/sql/driver"
)

type Option func(*Driver)

// WithLogger sets the logger for database modifications.
func WithLogger(logger Logger) Option {
	return func(d *Driver) {
		d.logger = logger
	}
}

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

func WithReadOnly(readOnly bool) Option {
	return func(d *Driver) {
		d.readOnly = readOnly
	}
}

// Driver is a wrapper around a standard SQL driver that logs database modifications.
// It implements the driver.Driver interface and provides additional functionality for auditing.
type Driver struct {
	driver.Driver
	builder  *databaseModificationBuilder
	readOnly bool
	logger   Logger
}

// NewDriver creates a new audit driver from a driver.Driver
func NewDriver(d driver.Driver, options ...Option) driver.Driver {
	return newAuditDriver(d, options...)
}

// NewConnector creates a new audit driver from a driver.Connector
func NewConnector(c driver.Connector, options ...Option) driver.Driver {
	return newAuditDriver(c.Driver(), options...)
}

func New(d interface{}, options ...Option) driver.Driver {
	var baseDriver driver.Driver

	switch v := d.(type) {
	case driver.Driver:
		baseDriver = v
	case driver.Connector:
		baseDriver = v.Driver()
	default:
		panic("audriver.New: argument must be driver.Driver or driver.Connector")
	}

	return newAuditDriver(baseDriver, options...)
}

func newAuditDriver(d driver.Driver, options ...Option) driver.Driver {
	drv := &Driver{
		Driver:  d,
		builder: &databaseModificationBuilder{},
	}

	for _, option := range options {
		option(drv)
	}

	drv.builder.fillDefaults()

	if drv.logger == nil {
		drv.logger = &noopLogger{}
	}

	return drv
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return &Conn{Conn: conn, builder: d.builder, readOnly: d.readOnly, logger: d.logger}, nil
}

var (
	_ driver.Driver = (*Driver)(nil)
)
