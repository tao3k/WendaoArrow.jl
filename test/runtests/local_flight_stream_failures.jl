@testset "Flight stream service rejects empty exchange payload" begin
    service = WendaoArrow.build_stream_flight_service(identity)
    request = Channel{Arrow.Flight.Protocol.FlightData}(1)
    close(request)
    response = Channel{Arrow.Flight.Protocol.FlightData}(1)

    @test_throws ArgumentError Arrow.Flight.doexchange(
        service,
        Arrow.Flight.ServerCallContext(),
        request,
        response,
    )
end
