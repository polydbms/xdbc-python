#include "../../include/xdbc.h"
#include <iostream>



int main() {

    xdbc::XClient c("Test Client");

    std::cout << "Made XClient called: " << c.get_name() << std::endl;

    //c.load("lalala");
    //c.receive();


    return 0;
}