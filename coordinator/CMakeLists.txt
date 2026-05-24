add_executable(
	mtorrent_server
	src/main.cpp
	src/server/coordinator.cpp
	src/services/coordinator_service.cpp
	src/utils/string_utils.cpp
	src/utils/socket_utils.cpp
)

target_link_libraries(mtorrent_server PRIVATE pthread)

target_include_directories(
	mtorrent_server
	PRIVATE
	include
)
