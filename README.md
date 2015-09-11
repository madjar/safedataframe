Safe Data Frame
---------------

An experimentation to provide type-safety to dataframes. Programming error should be compile-time error when possible, not runtime.

Here is an example of the kind of things I'm trying to do:

    val df = sqlContext.read.json("people.json")
    val sdf = SafeDataFrame(df, "name".witness :: HNil)  // Check at runtime the "name" column exists
    sdf("name".witness)
    sdf("notacolumn".witness)                            // Does not compile

This does not compile, because "notacolumn" does not exist.