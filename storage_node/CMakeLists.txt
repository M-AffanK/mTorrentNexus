add_executable(
	mtorrent_storage_node
	src/main.cpp
	src/server/storage_node.cpp
	src/services/chunk_store.cpp
	src/utils/socket_utils.cpp
	src/utils/string_utils.cpp
)

target_link_libraries(mtorrent_storage_node PRIVATE pthread)

target_include_directories(
	mtorrent_storage_node
	PRIVATE
	include
)
