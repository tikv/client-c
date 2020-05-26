find_package (Poco REQUIRED Foundation Net JSON Util)

if (Poco_FOUND)
	message(STATUS "Using Poco: ${Poco_VERSION}, ${Poc_LIBRARIES}")
else ()
	message(STATUS "Poco Not Found")
endif()
