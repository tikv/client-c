find_package (Poco REQUIRED Foundation Net Json Util)

if (Poco_FOUND)
    message(STATUS "Using Poco: ${Poco_VERSION}, ${Poco_LIBRARIES}")
else()
    message(STATUS "Poco Not Found")
endif()
