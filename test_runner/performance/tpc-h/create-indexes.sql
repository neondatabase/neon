-- Section 1.4.2.2

ALTER TABLE part ADD PRIMARY KEY (p_partkey);
ALTER TABLE supplier ADD PRIMARY KEY (s_suppkey);
ALTER TABLE partsupp ADD PRIMARY KEY (ps_partkey, ps_suppkey);
ALTER TABLE customer ADD PRIMARY KEY (c_custkey);
ALTER TABLE orders ADD PRIMARY KEY (o_orderkey);
ALTER TABLE lineitem ADD PRIMARY KEY (l_orderkey, l_linenumber);
ALTER TABLE nation ADD PRIMARY KEY (n_nationkey);
ALTER TABLE region ADD PRIMARY KEY (r_regionkey);

-- Section 1.4.2.3

CREATE INDEX ON supplier USING btree (s_nationkey);
ALTER TABLE supplier ADD FOREIGN KEY (s_nationkey) REFERENCES nation (n_nationkey);

/* IGNORE: implied by primary key */
-- CREATE INDEX ON partsupp USING btree (ps_partkey);
CREATE INDEX ON partsupp USING btree (ps_suppkey);
ALTER TABLE partsupp ADD FOREIGN KEY (ps_partkey) REFERENCES part (p_partkey);
ALTER TABLE partsupp ADD FOREIGN KEY (ps_suppkey) REFERENCES supplier (s_suppkey);

CREATE INDEX ON customer USING btree (c_nationkey);
ALTER TABLE customer ADD FOREIGN KEY (c_nationkey) REFERENCES nation (n_nationkey);

CREATE INDEX ON orders USING btree (o_custkey);
ALTER TABLE orders ADD FOREIGN KEY (o_custkey) REFERENCES customer (c_custkey);

/* IGNORE: implied by primary key */
-- CREATE INDEX ON lineitem USING btree (l_orderkey);
CREATE INDEX ON lineitem USING btree (l_partkey, l_suppkey);
CREATE INDEX ON lineitem USING btree (l_suppkey);
ALTER TABLE lineitem ADD FOREIGN KEY (l_orderkey) REFERENCES orders (o_orderkey);
ALTER TABLE lineitem ADD FOREIGN KEY (l_partkey) REFERENCES part (p_partkey);
ALTER TABLE lineitem ADD FOREIGN KEY (l_suppkey) REFERENCES supplier (s_suppkey);
ALTER TABLE lineitem ADD FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp (ps_partkey, ps_suppkey);

CREATE INDEX ON nation USING btree (n_regionkey);
ALTER TABLE nation ADD FOREIGN KEY (n_regionkey) REFERENCES region (r_regionkey);

-- Section 1.4.2.4

ALTER TABLE lineitem ADD CHECK (l_shipdate <= l_receiptdate);
