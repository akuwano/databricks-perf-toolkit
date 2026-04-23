"""Column-type extraction from EXPLAIN EXTENDED.

Covers:
- ReadSchema: struct<col:type,...> parsing (decimal, nested, etc.)
- scan_schemas: dict[table_name, dict[col, type]] on ExplainExtended
"""

from core.explain_parser import parse_explain_extended


class TestReadSchemaExtraction:
    def test_simple_struct(self):
        text = """== Physical Plan ==
PhotonScan parquet main.base.t[a#1,b#2] ReadSchema: struct<a:int,b:bigint>
"""
        res = parse_explain_extended(text)
        schema = res.scan_schemas.get("main.base.t", {})
        assert schema == {"a": "int", "b": "bigint"}

    def test_decimal_type_with_comma_in_precision(self):
        text = """== Physical Plan ==
PhotonScan parquet main.base.sales[price#1,qty#2] ReadSchema: struct<price:decimal(38,2),qty:int>
"""
        res = parse_explain_extended(text)
        schema = res.scan_schemas.get("main.base.sales", {})
        assert schema.get("price") == "decimal(38,2)"
        assert schema.get("qty") == "int"

    def test_multiple_scans_merged_by_table(self):
        text = """== Physical Plan ==
PhotonScan parquet main.base.t1[a#1] ReadSchema: struct<a:int>
PhotonScan parquet main.base.t2[b#2] ReadSchema: struct<b:string>
"""
        res = parse_explain_extended(text)
        assert res.scan_schemas.get("main.base.t1") == {"a": "int"}
        assert res.scan_schemas.get("main.base.t2") == {"b": "string"}

    def test_string_and_date_types(self):
        text = """== Physical Plan ==
PhotonScan parquet main.base.t[n#1,d#2,ts#3] ReadSchema: struct<n:string,d:date,ts:timestamp>
"""
        res = parse_explain_extended(text)
        schema = res.scan_schemas.get("main.base.t", {})
        assert schema == {"n": "string", "d": "date", "ts": "timestamp"}

    def test_no_readschema_means_empty_dict(self):
        text = """== Physical Plan ==
PhotonScan parquet main.base.t[a#1]
"""
        res = parse_explain_extended(text)
        assert res.scan_schemas.get("main.base.t") in (None, {})


class TestReadSchemaInOptimizedLogicalPlan:
    """Regression: Databricks sometimes places PhotonScan (with ReadSchema)
    under the Optimized Logical Plan section rather than the Physical Plan.

    Captured from a real production profile (analysis 8d970c04-...) where
    scan_schemas came out empty because the parser only ran extraction on
    PHYSICAL sections and the family classifier labeled logical-section
    PhotonScans as UNKNOWN.
    """

    def test_readschema_under_optimized_logical_plan(self):
        text = """== Optimized Logical Plan ==
PhotonScan parquet skato.aisin_poc.customer_delta_lc[C_CUSTOMER_SK#13062] DataFilters: [isnotnull(C_CUSTOMER_SK#13062)], DictionaryFilters: [], Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[...], OptionalDataFilters: [], PartitionFilters: [], ReadSchema: struct<C_CUSTOMER_SK:decimal(38,0)>, RequiredDataFilters: [isnotnull(C_CUSTOMER_SK#13062)]
"""
        res = parse_explain_extended(text)
        # Must still pick up ReadSchema even from the logical section
        assert (
            res.scan_schemas.get("skato.aisin_poc.customer_delta_lc", {}).get("C_CUSTOMER_SK")
            == "decimal(38,0)"
        )

    def test_multiple_scans_with_full_line_noise(self):
        """All clustering tables in the real analysis came out with
        decimal(38,0) keys. Verify all three tables resolve."""
        text = """== Optimized Logical Plan ==
PhotonScan parquet skato.aisin_poc.store_sales_delta_lc[SS_CUSTOMER_SK#13279,SS_QUANTITY#13286] DataFilters: [isnotnull(SS_CUSTOMER_SK#13279)], Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[...], PartitionFilters: [], ReadSchema: struct<SS_CUSTOMER_SK:decimal(38,0),SS_QUANTITY:int>, RequiredDataFilters: []
PhotonScan parquet skato.aisin_poc.customer_delta_lc[C_CUSTOMER_SK#13299] Format: parquet, ReadSchema: struct<C_CUSTOMER_SK:decimal(38,0)>, RequiredDataFilters: []
"""
        res = parse_explain_extended(text)
        ss_schema = res.scan_schemas.get("skato.aisin_poc.store_sales_delta_lc", {})
        c_schema = res.scan_schemas.get("skato.aisin_poc.customer_delta_lc", {})
        assert ss_schema.get("SS_CUSTOMER_SK") == "decimal(38,0)"
        assert ss_schema.get("SS_QUANTITY") == "int"
        assert c_schema.get("C_CUSTOMER_SK") == "decimal(38,0)"


class TestIntegrationQ23LikePlan:
    def test_join_key_type_resolvable_via_readschema(self):
        text = """== Physical Plan ==
PhotonShuffledHashJoin [ss_customer_sk#8], [c_customer_sk#100], Inner, BuildRight
+- PhotonScan parquet main.base.store_sales[ss_customer_sk#8,ss_quantity#14] ReadSchema: struct<ss_customer_sk:decimal(38,0),ss_quantity:int>
+- PhotonScan parquet main.base.customer[c_customer_sk#100] ReadSchema: struct<c_customer_sk:bigint>
"""
        res = parse_explain_extended(text)
        # store_sales.ss_customer_sk is decimal(38,0)
        assert res.scan_schemas["main.base.store_sales"]["ss_customer_sk"] == "decimal(38,0)"
        # customer.c_customer_sk is bigint
        assert res.scan_schemas["main.base.customer"]["c_customer_sk"] == "bigint"
        # This is the exact situation the LLM needs to flag — a mismatch
        # between decimal and bigint on JOIN keys.
