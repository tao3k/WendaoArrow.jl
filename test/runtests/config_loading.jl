@testset "config loading merges TOML and flags" begin
    config_path = tempname() * ".toml"
    write(
        config_path,
        """
        [interface]
        host = "0.0.0.0"
        port = 18815
        """,
    )

    config = WendaoArrow.config_from_args(["--config", config_path, "--port", "19090"])

    @test config.host == "0.0.0.0"
    @test config.port == 19090
end

@testset "config loading rejects removed HTTP flags" begin
    err = try
        WendaoArrow.config_from_args(["--route", "/arrow-ipc"])
        nothing
    catch error
        error
    end

    @test err isa ArgumentError
    @test occursin("unsupported argument: --route", sprint(showerror, err))
end
