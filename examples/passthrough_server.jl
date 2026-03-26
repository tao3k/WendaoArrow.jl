using Pkg

Pkg.activate(joinpath(@__DIR__, ".."))

using Tables
using WendaoArrow

processor(table) = Tables.columntable(table)
config = WendaoArrow.config_from_args(ARGS)

WendaoArrow.serve(processor; config = config)
