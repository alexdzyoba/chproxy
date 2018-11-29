delay = function()
    return 1000
end

request = function()
    query = string.format("SELECT %d HTTP/1.1", math.random(1,100))
    return wrk.format("POST", "/", wrk.header, query)
end
