import argparse
import hashlib
from pyspark.sql import SparkSession, functions as F, types as T


QUERY_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("query_number", T.IntegerType(), False),
    T.StructField("query", T.StringType(), False),
]))


def clean(value):
    if value is None:
        return ""
    value = str(value).strip()
    return "" if value.lower() in {"nan", "none", "null", "n/a", "-"} else value


def build_queries(first, last, company, title, location):
    first = clean(first)
    last = clean(last)
    company = clean(company)
    title = clean(title)
    location = clean(location)

    full_name = " ".join(x for x in [first, last] if x)
    if not full_name:
        return []

    context = company or location or title
    quoted_context = f'"{context}"' if context else ""

    query_one = f'(site:x.com OR site:twitter.com) "{full_name}" {quoted_context}'.strip()

    if company and location:
        query_two = (
            f'(site:x.com OR site:twitter.com) "{full_name}" '
            f'"{company}" {location}'
        )
    elif title:
        query_two = (
            f'(site:x.com OR site:twitter.com) "{full_name}" '
            f'"{title}"'
        )
    else:
        query_two = f'(site:x.com OR site:twitter.com) "{full_name}"'

    if query_two == query_one:
        query_two = f'(site:x.com OR site:twitter.com) "{full_name}" twitter'

    return [
        {"query_number": 1, "query": query_one},
        {"query_number": 2, "query": query_two},
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--input-format", choices=["csv", "parquet"], default="csv")
    parser.add_argument("--partitions", type=int, default=200)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("TwitterXPrepareQueries")
        .getOrCreate()
    )

    if args.input_format == "parquet":
        contacts = spark.read.parquet(args.input)
    else:
        contacts = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(args.input)
        )

    columns = set(contacts.columns)

    def get_column(name, fallback=""):
        return F.col(name) if name in columns else F.lit(fallback)

    contact_id = (
        F.when(
            get_column("ic_cntuid").isNotNull()
            & (get_column("ic_cntuid") != ""),
            get_column("ic_cntuid").cast("string"),
        )
        .otherwise(
            F.sha2(
                F.concat_ws(
                    "|",
                    get_column("ic_fname", ""),
                    get_column("ic_lname", ""),
                    get_column("email", ""),
                    get_column("ic_company", ""),
                ),
                256,
            )
        )
    )

    contacts = contacts.withColumn("contact_id", contact_id)

    build_udf = F.udf(build_queries, QUERY_SCHEMA)

    queries = (
        contacts
        .select(
            "contact_id",
            get_column("ic_fname", "").cast("string").alias("first_name"),
            get_column("ic_lname", "").cast("string").alias("last_name"),
            get_column("ic_company", "").cast("string").alias("company"),
            get_column("ic_jtitle", "").cast("string").alias("title"),
            get_column("Location", "").cast("string").alias("location"),
        )
        .withColumn(
            "query_records",
            build_udf(
                "first_name",
                "last_name",
                "company",
                "title",
                "location",
            ),
        )
        .select("contact_id", F.explode("query_records").alias("record"))
        .select(
            "contact_id",
            F.col("record.query_number").alias("query_number"),
            F.col("record.query").alias("query"),
        )
        .withColumn(
            "query_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    "contact_id",
                    F.col("query_number").cast("string"),
                    "query",
                ),
                256,
            ),
        )
        .repartition(args.partitions, "query_id")
    )

    queries.write.mode("overwrite").parquet(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
