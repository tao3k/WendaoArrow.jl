function normalize_metadata_values(
    metadata;
    string_keys = (),
    int64_keys = (),
    bool_keys = (),
    float64_keys = (),
    datetime_keys = (),
    enum_string_keys = (),
    enum_keys = (),
    subject::AbstractString = "WendaoArrow request metadata",
)
    normalized_string_keys = _normalize_required_columns(string_keys)
    normalized_int64_keys = _normalize_required_columns(int64_keys)
    normalized_bool_keys = _normalize_required_columns(bool_keys)
    normalized_float64_keys = _normalize_required_columns(float64_keys)
    normalized_datetime_keys = _normalize_required_columns(datetime_keys)
    normalized_enum_string_specs = _normalize_enum_string_specs(
        enum_string_keys;
        subject = subject,
        group_label = "enum string metadata keys",
    )
    normalized_enum_string_keys = [first(spec) for spec in normalized_enum_string_specs]
    normalized_enum_specs = _normalize_enum_type_specs(
        enum_keys;
        subject = subject,
        group_label = "enum metadata keys",
    )
    normalized_enum_keys = [first(spec) for spec in normalized_enum_specs]
    counts = Dict{String,Int}()
    for key in (
        normalized_string_keys...,
        normalized_int64_keys...,
        normalized_bool_keys...,
        normalized_float64_keys...,
        normalized_datetime_keys...,
        normalized_enum_string_keys...,
        normalized_enum_keys...,
    )
        counts[key] = get(counts, key, 0) + 1
    end
    duplicate_keys = sort!([key for (key, count) in counts if count > 1])
    isempty(duplicate_keys) || throw(
        ArgumentError(
            "$(subject) declares duplicate metadata keys across type groups: $(join(duplicate_keys, ", "))",
        ),
    )

    normalized = Dict{String,Any}()
    for key in normalized_string_keys
        normalized[key] = coerce_metadata_optional_string(metadata, key; subject = subject)
    end
    for key in normalized_int64_keys
        normalized[key] = coerce_metadata_optional_int64(metadata, key; subject = subject)
    end
    for key in normalized_bool_keys
        normalized[key] = coerce_metadata_optional_bool(metadata, key; subject = subject)
    end
    for key in normalized_float64_keys
        normalized[key] = coerce_metadata_optional_float64(metadata, key; subject = subject)
    end
    for key in normalized_datetime_keys
        normalized[key] =
            coerce_metadata_optional_datetime(metadata, key; subject = subject)
    end
    for (key, allowed_values) in normalized_enum_string_specs
        normalized[key] = coerce_metadata_optional_enum_string(
            metadata,
            key;
            subject = subject,
            allowed_values = allowed_values,
        )
    end
    for (key, enum_type) in normalized_enum_specs
        normalized[key] = coerce_metadata_optional_enum(
            metadata,
            key;
            subject = subject,
            enum_type = enum_type,
        )
    end
    return normalized
end

function coerce_metadata_optional_string(
    metadata,
    key::AbstractString;
    subject::AbstractString = "WendaoArrow request metadata",
)
    value = get(metadata, key, missing)
    ismissing(value) && return missing

    if value isa AbstractString
        result = String(value)
        isempty(result) && throw(
            ArgumentError(
                "$(subject) key $(key) must contain non-empty string values or be missing; got empty string",
            ),
        )
        return result
    end

    throw(
        ArgumentError(
            "$(subject) key $(key) must contain string values or be missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end

function coerce_metadata_optional_int64(
    metadata,
    key::AbstractString;
    subject::AbstractString = "WendaoArrow request metadata",
)
    value = get(metadata, key, missing)
    ismissing(value) && return missing

    if value isa Integer
        result = try
            Int64(value)
        catch
            throw(
                ArgumentError(
                    "$(subject) key $(key) must contain Int64 values or be missing; got $(repr(value))::$(typeof(value))",
                ),
            )
        end
        return result
    end

    if value isa AbstractString
        text = String(value)
        isempty(text) && throw(
            ArgumentError(
                "$(subject) key $(key) must contain Int64 values or be missing; got empty string",
            ),
        )
        result = try
            parse(Int64, text)
        catch
            throw(
                ArgumentError(
                    "$(subject) key $(key) must contain Int64 values or be missing; got $(repr(value))::$(typeof(value))",
                ),
            )
        end
        return result
    end

    throw(
        ArgumentError(
            "$(subject) key $(key) must contain Int64 values or be missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end

function coerce_metadata_optional_bool(
    metadata,
    key::AbstractString;
    subject::AbstractString = "WendaoArrow request metadata",
)
    value = get(metadata, key, missing)
    ismissing(value) && return missing

    if value isa Bool
        return value
    end

    if value isa AbstractString
        text = String(value)
        isempty(text) && throw(
            ArgumentError(
                "$(subject) key $(key) must contain Bool values or be missing; got empty string",
            ),
        )
        normalized = lowercase(text)
        normalized == "true" && return true
        normalized == "false" && return false
    end

    throw(
        ArgumentError(
            "$(subject) key $(key) must contain Bool values or be missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end

function coerce_metadata_optional_float64(
    metadata,
    key::AbstractString;
    subject::AbstractString = "WendaoArrow request metadata",
)
    value = get(metadata, key, missing)
    ismissing(value) && return missing

    if value isa Real && !(value isa Bool)
        result = try
            Float64(value)
        catch
            throw(
                ArgumentError(
                    "$(subject) key $(key) must contain finite Float64 values or be missing; got $(repr(value))::$(typeof(value))",
                ),
            )
        end
        isfinite(result) && return result
        throw(
            ArgumentError(
                "$(subject) key $(key) must contain finite Float64 values or be missing; got $(repr(result))::$(typeof(result))",
            ),
        )
    end

    if value isa AbstractString
        text = String(value)
        isempty(text) && throw(
            ArgumentError(
                "$(subject) key $(key) must contain finite Float64 values or be missing; got empty string",
            ),
        )
        result = try
            parse(Float64, text)
        catch
            throw(
                ArgumentError(
                    "$(subject) key $(key) must contain finite Float64 values or be missing; got $(repr(value))::$(typeof(value))",
                ),
            )
        end
        isfinite(result) && return result
        throw(
            ArgumentError(
                "$(subject) key $(key) must contain finite Float64 values or be missing; got $(repr(result))::$(typeof(result))",
            ),
        )
    end

    throw(
        ArgumentError(
            "$(subject) key $(key) must contain finite Float64 values or be missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end

function coerce_metadata_optional_datetime(
    metadata,
    key::AbstractString;
    subject::AbstractString = "WendaoArrow request metadata",
)
    value = get(metadata, key, missing)
    ismissing(value) && return missing

    value isa DateTime && return value

    if value isa AbstractString
        text = String(value)
        isempty(text) && throw(
            ArgumentError(
                "$(subject) key $(key) must contain ISO8601 DateTime values or be missing; got empty string",
            ),
        )
        result = try
            DateTime(text)
        catch
            throw(
                ArgumentError(
                    "$(subject) key $(key) must contain ISO8601 DateTime values or be missing; got $(repr(value))::$(typeof(value))",
                ),
            )
        end
        return result
    end

    throw(
        ArgumentError(
            "$(subject) key $(key) must contain ISO8601 DateTime values or be missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end

function coerce_metadata_optional_enum_string(
    metadata,
    key::AbstractString;
    subject::AbstractString = "WendaoArrow request metadata",
    allowed_values,
)
    value = get(metadata, key, missing)
    ismissing(value) && return missing
    expected = _enum_values_label(_normalize_required_columns(allowed_values))

    if value isa AbstractString
        result = String(value)
        isempty(result) && throw(
            ArgumentError(
                "$(subject) key $(key) must contain one of $(expected) or be missing; got empty string",
            ),
        )
        result in allowed_values && return result
    end

    throw(
        ArgumentError(
            "$(subject) key $(key) must contain one of $(expected) or be missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end

function coerce_metadata_optional_enum(
    metadata,
    key::AbstractString;
    subject::AbstractString = "WendaoArrow request metadata",
    enum_type::Type{T},
) where {T<:Enum}
    value = get(metadata, key, missing)
    ismissing(value) && return missing
    expected = _enum_values_label(_enum_value_labels(T))

    value isa T && return value

    if value isa Integer && !(value isa Bool)
        try
            return T(value)
        catch
        end
    end

    if value isa AbstractString
        text = String(value)
        isempty(text) && throw(
            ArgumentError(
                "$(subject) key $(key) must contain one of $(expected) or be missing; got empty string",
            ),
        )
        for instance in instances(T)
            text == string(instance) && return instance
        end
    end

    throw(
        ArgumentError(
            "$(subject) key $(key) must contain one of $(expected) or be missing; got $(repr(value))::$(typeof(value))",
        ),
    )
end
