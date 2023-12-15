
class BulkInsertMapper:
    BLOB = "%s"
    DATE = "CAST(%s AS DATE)"
    INTERVAL = "%s"
    NCLOB = "%s"
    NUMBER = "%s"
    TIMESTAMP = "CAST(%s AS TIMESTAMP)"

    types = {
        "AutoField": NUMBER,
        "BigAutoField": NUMBER,
        "BigIntegerField": NUMBER,
        "BinaryField": BLOB,
        "BooleanField": NUMBER,
        "DateField": DATE,
        "DateTimeField": TIMESTAMP,
        "DecimalField": NUMBER,
        "DurationField": INTERVAL,
        "FloatField": NUMBER,
        "IntegerField": NUMBER,
        "PositiveBigIntegerField": NUMBER,
        "PositiveIntegerField": NUMBER,
        "PositiveSmallIntegerField": NUMBER,
        "SmallAutoField": NUMBER,
        "SmallIntegerField": NUMBER,
        "TextField": NCLOB,
        "TimeField": TIMESTAMP,
    }
