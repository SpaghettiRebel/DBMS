#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    std::string command;

    while (true) {
        std::cout << "dbms> " << std::flush;

        if (!std::getline(std::cin, command)) {
            break;
        }

        if (command == "exit") {
            break;
        }

        std::cout << "Executing..." << std::endl;
    }

    return 0;
}
