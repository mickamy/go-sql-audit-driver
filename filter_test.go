package audriver_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-txdb"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mickamy/audriver"
)

// TestAuditDriver_TableFilters tests table filtering functionality
func TestAuditDriver_TableFilters(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	opID := uuid.New()
	execID := uuid.New()
	ctx = audriver.WithOperatorID(ctx, opID.String())
	ctx = audriver.WithExecutionID(ctx, execID.String())

	testCases := []struct {
		name           string
		filters        []audriver.TableFilter
		tableName      string
		shouldBeLogged bool
	}{
		{
			name:           "no_filters_logs_everything",
			filters:        nil,
			tableName:      "users",
			shouldBeLogged: true,
		},
		{
			name:           "exclude_prefix_filter",
			filters:        []audriver.TableFilter{audriver.NewExcludePrefixFilter("temp_", "log_")},
			tableName:      "temp_users",
			shouldBeLogged: false,
		},
		{
			name:           "exclude_prefix_filter_allows_others",
			filters:        []audriver.TableFilter{audriver.NewExcludePrefixFilter("temp_", "log_")},
			tableName:      "users",
			shouldBeLogged: true,
		},
		{
			name:           "include_pattern_filter",
			filters:        []audriver.TableFilter{audriver.NewIncludePatternFilter("user*", "order*")},
			tableName:      "users",
			shouldBeLogged: true,
		},
		{
			name:           "include_pattern_filter_excludes_others",
			filters:        []audriver.TableFilter{audriver.NewIncludePatternFilter("user*", "order*")},
			tableName:      "products",
			shouldBeLogged: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// arrange
			driverName := fmt.Sprintf("filter_test_%s_%d", tc.name, gofakeit.Number(1000, 9999))
			baseDriver := txdb.New("postgres", writerDSN)

			var auditDriver driver.Driver
			if tc.filters != nil {
				auditDriver = audriver.New(baseDriver, audriver.WithTableFilters(tc.filters...))
			} else {
				auditDriver = audriver.New(baseDriver)
			}

			sql.Register(driverName, auditDriver)
			db, err := sql.Open(driverName, driverName)
			require.NoError(t, err)
			defer func(db *sql.DB) {
				_ = db.Close()
			}(db)

			if tc.filters != nil {
				// act
				filters := audriver.TableFilters(tc.filters)
				result := filters.ShouldLog(tc.tableName)

				// assert
				assert.Equal(t, tc.shouldBeLogged, result, "filter should return expected result for table: %s", tc.tableName)
			}
		})
	}
}
