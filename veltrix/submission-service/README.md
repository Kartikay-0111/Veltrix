Submission Requirements
───────────────────────
1. Submit a .tar.gz of your project root
2. Must contain CMakeLists.txt at the root
3. CMake must produce a binary named exactly: server
4. server must listen on port 9999
5. server must handle:
     POST /order
     DELETE /order/{id}
     GET  /book/{ticker}
     GET  /health

Example CMakeLists.txt:
   add_executable(server main.cpp orderbook.cpp)
   target_compile_features(server PRIVATE cxx_std_20)