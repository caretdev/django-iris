from django.core.exceptions import EmptyResultSet, FullResultSet
from django.db.models.expressions import Col, Value
from django.db.models.sql import compiler

class SQLCompiler(compiler.SQLCompiler):

    def as_sql(self, with_limits=True, with_col_aliases=False):
        with_limit_offset = (with_limits or self.query.is_sliced) and (
            self.query.high_mark is not None or self.query.low_mark > 0
        )
        if self.query.select_for_update or not with_limit_offset:
            return super().as_sql(with_limits, with_col_aliases)
        try:
            extra_select, order_by, group_by = self.pre_sql_setup()

            offset = self.query.low_mark + 1 if self.query.low_mark else 0
            limit = self.query.high_mark

            distinct_fields, distinct_params = self.get_distinct()
            # This must come after 'select', 'ordering', and 'distinct'
            # (see docstring of get_from_clause() for details).
            from_, f_params = self.get_from_clause()
            try:
                where, w_params = (
                    self.compile(self.where) if self.where is not None else ("", [])
                )
            except EmptyResultSet:
                if self.elide_empty:
                    raise
                # Use a predicate that's always False.
                where, w_params = "0 = 1", []
            except FullResultSet:
                where, w_params = "", []
            having, h_params = (
                self.compile(self.having) if self.having is not None else ("", [])
            )
            result = ["SELECT"]
            params = []

            if self.query.distinct:
                distinct_result, distinct_params = self.connection.ops.distinct_sql(
                    distinct_fields,
                    distinct_params,
                )
                result += distinct_result
                params += distinct_params

            if not offset:
                result.append("TOP %d" % limit)

            first_col = ""
            out_cols = []
            col_idx = 1
            for _, (s_sql, s_params), alias in self.select + extra_select:
                first_col = s_sql if not first_col else first_col
                if alias:
                    s_sql = "%s AS %s" % (
                        s_sql,
                        self.connection.ops.quote_name(alias),
                    )
                elif with_col_aliases:
                    s_sql = "%s AS %s" % (
                        s_sql,
                        self.connection.ops.quote_name("col%d" % col_idx),
                    )
                    col_idx += 1
                params.extend(s_params)
                out_cols.append(s_sql)

            order_by_result = ""

            if order_by:
                ordering = []
                for _, (o_sql, o_params, _) in order_by:
                    ordering.append(o_sql)
                    params.extend(o_params)
                order_by_result = "ORDER BY %s" % ", ".join(ordering)
            elif offset:
                order_by_result = "ORDER BY %s" % first_col

            if offset:
                out_cols.append("ROW_NUMBER() %s AS row_number" % ("OVER (%s)" % order_by_result if order_by_result else ""))

            result += [", ".join(out_cols), "FROM", *from_]
            params.extend(f_params)

            if where:
                result.append("WHERE %s" % where)
                params.extend(w_params)

            grouping = []
            for g_sql, g_params in group_by:
                grouping.append(g_sql)
                params.extend(g_params)
            if grouping:
                if distinct_fields:
                    raise NotImplementedError(
                        "annotate() + distinct(fields) is not implemented."
                    )
                order_by = order_by or self.connection.ops.force_no_ordering()
                result.append("GROUP BY %s" % ", ".join(grouping))
                if self._meta_ordering:
                    order_by = None
            if having:
                result.append("HAVING %s" % having)
                params.extend(h_params)

            if self.query.explain_info:
                result.insert(
                    0,
                    self.connection.ops.explain_query_prefix(
                        self.query.explain_info.format,
                        **self.query.explain_info.options,
                    ),
                )

            if order_by_result and not offset:
                result.append(order_by_result)

            query = " ".join(result)

            if self.query.subquery and extra_select:
                # If the query is used as a subquery, the extra selects would
                # result in more columns than the left-hand side expression is
                # expecting. This can happen when a subquery uses a combination
                # of order_by() and distinct(), forcing the ordering expressions
                # to be selected as well. Wrap the query in another subquery
                # to exclude extraneous selects.
                sub_selects = []
                sub_params = []
                for index, (select, _, alias) in enumerate(self.select, start=1):
                    if not alias and with_col_aliases:
                        alias = "col%d" % index
                    if alias:
                        sub_selects.append(
                            "%s.%s"
                            % (
                                self.connection.ops.quote_name("subquery"),
                                self.connection.ops.quote_name(alias),
                            )
                        )
                    else:
                        select_clone = select.relabeled_clone(
                            {select.alias: "subquery"}
                        )
                        subselect, subparams = select_clone.as_sql(
                            self, self.connection
                        )
                        sub_selects.append(subselect)
                        sub_params.extend(subparams)
                query = "SELECT %s FROM (%s) subquery" % (
                    ", ".join(sub_selects),
                    query,
                ), tuple(sub_params + params)

            if offset:
                query = "SELECT * FROM (%s) WHERE row_number between %d AND %d ORDER BY row_number" % (
                    query,
                    offset,
                    limit,
                )
            return query, tuple(params)
        except:
            return super().as_sql(with_limits, with_col_aliases)


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    
    def as_sql(self):
        if self.query.fields:
            return super().as_sql()

        # No values provided

        qn = self.connection.ops.quote_name
        opts = self.query.get_meta()
        insert_statement = self.connection.ops.insert_statement(
            on_conflict=self.query.on_conflict,
        )
        result = ["%s %s" % (insert_statement, qn(opts.db_table))]
        fields = self.query.fields or [opts.pk]
        result.append("(%s)" % ", ".join(qn(f.column) for f in fields))
        result.append("DEFAULT VALUES")

        return [(" ".join(result), [])]


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    def as_sql(self):
        sql, params = super().as_sql()
        if self.connection._disable_constraint_checking:
            sql = "UPDATE %%NOCHECK" + sql[6:]
        return sql, params
    

class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
