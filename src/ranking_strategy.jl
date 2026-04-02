@enum CacheBackend::Int32 begin
    memory = 1
    disk = 2
    remote = 3
end

@enum CacheScope::Int32 begin
    request = 1
    tenant = 2
    var"global" = 3
end

@enum RankingStrategy::Int32 begin
    lexical = 1
    semantic = 2
    hybrid = 3
end

module LinkGraphRetrievalModes

@enum LinkGraphRetrievalMode::Int32 begin
    graph_only = 1
    hybrid = 2
    vector_only = 3
end

end

const LinkGraphRetrievalMode = LinkGraphRetrievalModes.LinkGraphRetrievalMode
