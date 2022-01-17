find_package (Poco REQUIRED Foundation Net JSON Util)

if (Poco_FOUND)
	message(STATUS "Using Poco: ${Poco_VERSION} : ${Poco_INCLUDE_DIRS}, ${Poco_LIBRARIES}")
else ()
	message(STATUS "Poco Not Found")
endif()
