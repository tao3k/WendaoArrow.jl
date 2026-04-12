@testset "cross-process Flight startup script serves parser-summary-like responses" begin
    with_parser_summary_like_flight_server() do port, process
        @test Base.process_running(process)
        response = native_julia_doexchange(
            port;
            max_send_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
            max_recieve_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
        )
        @test length(response.item_group) ==
              WendaoArrowExampleSupport.PARSER_SUMMARY_LIKE_ROW_COUNT
        @test response.module_name[1] == "Blocks"
        @test response.item_target_path[1] == "Modelica.Blocks.Interfaces.RealInput1"
        @test response.item_dependency_target[3] == "Modelica.Units.SI"
        @test response.item_modifier_names[5] == "unit,start"
        @test hasproperty(response, :item_parser_attr_24)
    end
end

@testset "WendaoArrow product helper reads parser-summary-like responses" begin
    with_parser_summary_like_flight_server() do port, process
        @test Base.process_running(process)
        response = product_helper_doexchange(
            port;
            max_send_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
            max_recieve_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
        )
        @test length(response.item_group) ==
              WendaoArrowExampleSupport.PARSER_SUMMARY_LIKE_ROW_COUNT
        @test response.item_group[2] == "documentation"
        @test occursin("parser summary excerpt", something(response.item_content[2], ""))
        @test response.item_parser_attr_02[2] == 4
    end
end

@testset "cross-process Flight accepts package.mo parser-summary request via native Julia source upload" begin
    with_parser_summary_request_flight_server() do port, process
        @test Base.process_running(process)
        response = native_julia_doexchange(
            port;
            source = parser_summary_request_table(),
            max_send_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
            max_recieve_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
        )
        @test length(response.item_group) ==
              WendaoArrowExampleSupport.PARSER_SUMMARY_LIKE_ROW_COUNT
        @test response.request_id[1] == "req-modelica-package"
        @test response.item_dependency_target[3] == "Modelica.Units.SI"
    end
end

@testset "cross-process Flight accepts package.mo parser-summary request over request channel" begin
    with_parser_summary_request_flight_server() do port, process
        @test Base.process_running(process)
        response = native_julia_channel_doexchange(
            port;
            source = parser_summary_request_table(),
            max_send_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
            max_recieve_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
        )
        @test length(response.item_group) ==
              WendaoArrowExampleSupport.PARSER_SUMMARY_LIKE_ROW_COUNT
        @test response.request_id[1] == "req-modelica-package"
    end
end

@testset "WendaoArrow product helper accepts package.mo parser-summary request" begin
    with_parser_summary_request_flight_server() do port, process
        @test Base.process_running(process)
        response = product_helper_doexchange(
            port;
            source = parser_summary_request_table(),
            max_send_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
            max_recieve_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
        )
        @test length(response.item_group) ==
              WendaoArrowExampleSupport.PARSER_SUMMARY_LIKE_ROW_COUNT
        @test response.request_id[1] == "req-modelica-package"
        @test occursin("parser summary excerpt", something(response.item_content[2], ""))
    end
end

@testset "WendaoArrow product helper separates cold start from warm package.mo transport" begin
    with_parser_summary_request_flight_server() do port, process
        @test Base.process_running(process)

        first_elapsed = @elapsed begin
            response = product_helper_doexchange(
                port;
                source = parser_summary_request_table(),
                max_send_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
                max_recieve_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
            )
            @test length(response.item_group) ==
                  WendaoArrowExampleSupport.PARSER_SUMMARY_LIKE_ROW_COUNT
        end

        second_elapsed = @elapsed begin
            response = product_helper_doexchange(
                port;
                source = parser_summary_request_table(),
                max_send_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
                max_recieve_message_length = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
            )
            @test length(response.item_group) ==
                  WendaoArrowExampleSupport.PARSER_SUMMARY_LIKE_ROW_COUNT
        end

        @test second_elapsed <= first_elapsed
    end
end
