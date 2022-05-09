package postgresql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/lib/pq"
)

const (
	// This returns the role membership for role, grant_role
	getAlterRoleQuery = `
SELECT rolname AS ALTER_ROLE, rolconfig AS ROLE_PARAMS
FROM pg_catalog.pg_roles pr
WHERE rolname = $1 AND array_to_string(rolconfig, ',') LIKE $2 = $3
`
)

//select rolname,rolconfig from pg_roles where rolname in ('[role_name]');

func resourcePostgreSQLAlterRole() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourcePostgreSQLAlterRoleCreate),
		Read:   PGResourceFunc(resourcePostgreSQLAlterRoleRead),
		Delete: PGResourceFunc(resourcePostgreSQLAlterRoleDelete),

		Schema: map[string]*schema.Schema{
			"alter_role": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the role to alter the attributes of",
			},
			"alter_parameter_key": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the parameter to alter on the role",
			},
			"alter_parameter_value": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The value of the parameter which is being set",
			},
		},
	}
}

func resourcePostgreSQLAlterRoleRead(db *DBConnection, d *schema.ResourceData) error {
	if !db.featureSupported(featurePrivileges) {
		return fmt.Errorf(
			"postgresql_alter_role resource is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	return readAlterRole(db, d)
}

func resourcePostgreSQLAlterRoleCreate(db *DBConnection, d *schema.ResourceData) error {
	if !db.featureSupported(featurePrivileges) {
		return fmt.Errorf(
			"postgresql_alter_role resource is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	txn, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	// Reset the role alterations before altering them again.
	if err = resetAlterRole(txn, d); err != nil {
		return err
	}

	if err = alterRole(txn, d); err != nil {
		return err
	}

	if err = txn.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	d.SetId(generateAlterRoleID(d))

	return readAlterRole(db, d)
}

func resourcePostgreSQLAlterRoleDelete(db *DBConnection, d *schema.ResourceData) error {
	if !db.featureSupported(featurePrivileges) {
		return fmt.Errorf(
			"postgresql_alter_role resource is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	txn, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	if err = resetAlterRole(txn, d); err != nil {
		return err
	}

	if err = txn.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

func readAlterRole(db QueryAble, d *schema.ResourceData) error {
	var alterRole, alterParameter, alterParameterValue string

	alterRoleID := d.Id()

	values := []interface{}{
		&alterRole,
		&alterParameter,
		&alterParameterValue,
	}

	err := db.QueryRow(getAlterRoleQuery, d.Get("alter_role"), d.Get("alter_parameter"), d.Get("alter_parameter_value")).Scan(values...)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] PostgreSQL alter role (%q) not found", alterRoleID)
		d.SetId("")
		return nil
	case err != nil:
		return fmt.Errorf("error reading alter role: %w", err)
	}

	d.Set("alter_role", alterRole)
	d.Set("alter_paramter", alterParameter)
	d.Set("alter_parameter_value", alterParameterValue)

	d.SetId(generateAlterRoleID(d))

	return nil
}

func createAlterRoleQuery(d *schema.ResourceData) string {
	alterRole, _ := d.Get("alter_role").(string)
	alterParameter, _ := d.Get("alter_role_parameter").(string)
	alterParameterValue, _ := d.Get("alter_role_parameter_value").(string)

	query := fmt.Sprintf(
		"ALTER ROLE %s SET %s TO %s",
		pq.QuoteIdentifier(alterRole),
		pq.QuoteIdentifier(alterParameter),
		pq.QuoteIdentifier(alterParameterValue),
	)

	return query
}

func createResetAlterRoleQuery(d *schema.ResourceData) string {
	alterRole, _ := d.Get("alter_role").(string)
	alterParameter, _ := d.Get("alter_role_parameter").(string)

	return fmt.Sprintf(
		"ALTER ROLE %s RESET %s",
		pq.QuoteIdentifier(alterRole),
		pq.QuoteIdentifier(alterParameter),
	)
}

func alterRole(txn *sql.Tx, d *schema.ResourceData) error {
	query := createAlterRoleQuery(d)
	if _, err := txn.Exec(query); err != nil {
		return fmt.Errorf("could not execute alter query: %w", err)
	}
	return nil
}

func resetAlterRole(txn *sql.Tx, d *schema.ResourceData) error {
	query := createResetAlterRoleQuery(d)
	if _, err := txn.Exec(query); err != nil {
		return fmt.Errorf("could not execute alter reset query: %w", err)
	}
	return nil
}

func generateAlterRoleID(d *schema.ResourceData) string {
	return strings.Join([]string{d.Get("alter_role").(string), d.Get("alter_parameter").(string), d.Get("alter_parameter_value").(string)}, "_")
}
