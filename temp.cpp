#include <bits/stdc++.h>
using namespace std;
// #include <vector>

enum Face
{
    N,
    S,
    E,
    W,
    Up,
    Down
};

class Spacecraft
{
private:
    int x, y, z;
    Face face;
    Face head; // Internal state variable for orientation

    Face getInitialHead(Face initial_face)
    {
        switch (initial_face)
        {
        case N:
            return Up;

        case S:
            return Up;

        case E:
            return Up;

        case W:
            return Up;

        case Up:
            return S;

        case Down:
            return N;

        default:
            break;
        }
    }

public:
    Spacecraft(int startX, int startY, int startZ, Face startface)
        : x(startX), y(startY), z(startZ), face(startface), head(getInitialHead(startface)) {}

    void moveForward()
    {
        if (head == Up)
        {
            switch (face)
            {
            case N:
                y++;
                break;
            case S:
                y--;
                break;
            case E:
                x++;
                break;
            case W:
                x--;
                break;
            }
        }
        else if (head == Down)
        {
            switch (face)
            {
            case N:
                y++;
                break;
            case S:
                y--;
                break;
            case E:
                x++;
                break;
            case W:
                x--;
                break;
            }
        }
        else if (head == N)
        {
            switch (face)
            {
            case Up:
                z++;
                break;
            case Down:
                z--;
                break;
            case E:
                x++;
                break;
            case W:
                x--;
                break;
            }
        }
        else if (head == S)
        {
            switch (face)
            {
            case Up:
                z++;
                break;
            case Down:
                z--;
                break;
            case E:
                x++;
                break;
            case W:
                x--;
                break;
            }
        }
        else if (head == E)
        {
            switch (face)
            {
            case Up:
                z++;
                break;
            case Down:
                z--;
                break;
            case N:
                y++;
                break;
            case S:
                y--;
                break;
            }
        }
        else if (head == W)
        {
            switch (face)
            {
            case N:
                y++;
                break;
            case S:
                y--;
                break;
            case Up:
                z++;
                break;
            case Down:
                z--;
                break;
            }
        }
    }

    void moveBackward()
    {
        if (head == Up)
        {
            switch (face)
            {
            case N:
                y--;
                break;
            case S:
                y++;
                break;
            case E:
                x--;
                break;
            case W:
                x++;
                break;
            }
        }
        else if (head == Down)
        {
            switch (face)
            {
            case N:
                y--;
                break;
            case S:
                y++;
                break;
            case E:
                x--;
                break;
            case W:
                x++;
                break;
            }
        }
        else if (head == N)
        {
            switch (face)
            {
            case Up:
                z--;
                break;
            case Down:
                z++;
                break;
            case E:
                x--;
                break;
            case W:
                x++;
                break;
            }
        }
        else if (head == S)
        {
            switch (face)
            {
            case Up:
                z--;
                break;
            case Down:
                z++;
                break;
            case E:
                x--;
                break;
            case W:
                x++;
                break;
            }
        }
        else if (head == E)
        {
            switch (face)
            {
            case Up:
                z--;
                break;
            case Down:
                z++;
                break;
            case N:
                y--;
                break;
            case S:
                y++;
                break;
            }
        }
        else if (head == W)
        {
            switch (face)
            {
            case N:
                y--;
                break;
            case S:
                y++;
                break;
            case Up:
                z--;
                break;
            case Down:
                z++;
                break;
            }
        }
    }

    void turnLeft()
    {
        if (head == Up)
        {
            switch (face)
            {
            case N:
                face = W;
                break;
            case S:
                face = E;
                break;
            case E:
                face = N;
                break;
            case W:
                face = S;
                break;
            }
        }
        else if (head == Down)
        {
            switch (face)
            {
            case N:
                face = E;
                break;
            case S:
                face = W;
                break;
            case E:
                face = S;
                break;
            case W:
                face = N;
                break;
            }
        }
        else if (head == N)
        {
            switch (face)
            {
            case Up:
                face = E;
                break;
            case Down:
                face = W;
                break;
            case E:
                face = Down;
                break;
            case W:
                face = Up;
                break;
            }
        }
        else if (head == S)
        {
            switch (face)
            {
            case Up:
                face = W;
                break;
            case Down:
                face = E;
                break;
            case E:
                face = Up;
                break;
            case W:
                face = Down;
                break;
            }
        }
        else if (head == E)
        {
            switch (face)
            {
            case Up:
                face = S;
                break;
            case Down:
                face = N;
                break;
            case N:
                face = Up;
                break;
            case S:
                face = Down;
                break;
            }
        }
        else if (head == W)
        {
            switch (face)
            {
            case Up:
                face = N;
                break;
            case Down:
                face = S;
                break;
            case N:
                face = Down;
                break;
            case S:
                face = Up;
                break;
            }
        }
    }

    void turnRight()
    {
        if (head == N)
        {
            switch (face)
            {
            case Up:
                face = W;
                break;
            case Down:
                face = E;
                break;
            case W:
                face = Down;
                break;
            case E:
                face = Up;
                break;
            }
        }
        else if (head == S)
        {
            switch (face)
            {
            case Up:
                face = E;
                break;
            case Down:
                face = W;
                break;
            case W:
                face = Up;
                break;
            case E:
                face = Down;
                break;
            }
        }
        else if (head == E)
        {
            switch (face)
            {
            case Up:
                face = N;
                break;
            case Down:
                face = S;
                break;
            case N:
                face = Down;
                break;
            case S:
                face = Up;
                break;
            }
        }
        else if (head == W)
        {
            switch (face)
            {
            case Up:
                face = S;
                break;
            case Down:
                face = W;
                break;
            case N:
                face = Up;
                break;
            case S:
                face = Down;
                break;
            }
        }
        else if (head == Up)
        {
            switch (face)
            {
            case N:
                face = E;
                break;
            case S:
                face = W;
                break;
            case E:
                face = S;
                break;
            case W:
                face = N;
                break;
            }
        }
        else if (head == Down)
        {
            switch (face)
            {
            case N:
                face = W;
                break;
            case S:
                face = E;
                break;
            case E:
                face = N;
                break;
            case W:
                face = S;
                break;
            }
        }
    }

    void turnUp()
    {
        if (head == N)
        {
            switch (face)
            {
            case Up:
                face = S;
                head = Up;
                break;
            case Down:
                face = N;
                head = Up;
                break;
            case W:
                face = N;
                head = E;
                break;
            case E:
                face = N;
                head = W;
                break;
            }
        }
        else if (head == S)
        {
            switch (face)
            {
            case Up:
                face = N;
                head = Up;
                break;
            case Down:
                face = S;
                head = Up;
                break;
            case W:
                face = S;
                head = E;
                break;
            case E:
                face = S;
                head = W;
                break;
            }
        }
        else if (head == E)
        {
            switch (face)
            {
            case Up:
                face = W;
                head = Up;
                break;
            case Down:
                face = E;
                head = Up;
                break;
            case N:
                face = E;
                head = S;
                break;
            case S:
                face = E;
                head = N;
                break;
            }
        }
        else if (head == W)
        {
            switch (face)
            {
            case Up:
                face = E;
                head = Up;
                break;
            case Down:
                face = W;
                head = Up;
                break;
            case N:
                face = W;
                head = S;
                break;
            case S:
                face = W;
                head = N;
                break;
            }
        }
        else if (head == Up)
        {
            switch (face)
            {
            case N:
                face = Up;
                head = S;
                break;
            case S:
                face = Up;
                head = N;
                break;
            case E:
                face = Up;
                head = W;
                break;
            case W:
                face = Up;
                head = E;
                break;
            }
        }
        else if (head == Down)
        {
            switch (face)
            {
            case N:
                face = Up;
                head = N;
                break;
            case S:
                face = Up;
                head = S;
                break;
            case E:
                face = Up;
                head = E;
                break;
            case W:
                face = Up;
                head = W;
                break;
            }
        }
    }

    void turnDown()
    {
        if (head == N)
        {
            switch (face)
            {
            case Up:
                face = N;
                head = Down;
                break;
            case Down:
                face = S;
                head = Down;
                break;
            case W:
                face = S;
                head = W;
                break;
            case E:
                face = S;
                head = E;
                break;
            }
        }
        else if (head == S)
        {
            switch (face)
            {
            case Up:
                face = S;
                head = Down;
                break;
            case Down:
                face = N;
                head = Down;
                break;
            case W:
                face = N;
                head = W;
                break;
            case E:
                face = N;
                head = E;
                break;
            }
        }
        else if (head == E)
        {
            switch (face)
            {
            case Up:
                face = E;
                head = Down;
                break;
            case Down:
                face = W;
                head = Down;
                break;
            case N:
                face = W;
                head = N;
                break;
            case S:
                face = W;
                head = S;
                break;
            }
        }
        else if (head == W)
        {
            switch (face)
            {
            case Up:
                face = W;
                head = Down;
                break;
            case Down:
                face = E;
                head = Down;
                break;
            case N:
                face = E;
                head = N;
                break;
            case S:
                face = E;
                head = S;
                break;
            }
        }
        else if (head == Up)
        {
            switch (face)
            {
            case N:
                face = Down;
                head = N;
                break;
            case S:
                face = Down;
                head = S;
                break;
            case E:
                face = Down;
                head = E;
                break;
            case W:
                face = Down;
                head = W;
                break;
            }
        }
        else if (head == Down)
        {
            switch (face)
            {
            case N:
                face = Down;
                head = S;
                break;
            case S:
                face = Down;
                head = N;
                break;
            case E:
                face = Down;
                head = W;
                break;
            case W:
                face = Down;
                head = E;
                break;
            }
        }
    }

    void printPositionAndface()
    {
        cout << "Final Position: (" << x << ", " << y << ", " << z << ")\n";
        cout << "Final face: ";
        switch (face)
        {
        case N:
            cout << "N";
            break;
        case S:
            cout << "S";
            break;
        case E:
            cout << "E";
            break;
        case W:
            cout << "W";
            break;
        case Up:
            cout << "Up";
            break;
        case Down:
            cout << "Down";
            break;
        }
        cout << "\n";
    }
};

int main()
{
    // Spacecraft chandrayaan3(0, 0, 0, Up);
    Spacecraft chandrayaan3(1, 1, 1, N);

    // vector<string> commands = {"f", "f", "l", "b", "d", "r"};
    // vector<string> commands = {"f", "r", "u", "b", "l", "f", "r", "b", "u", "l"};
    vector<string> commands = {"f", "l", "u", "b", "r", "f", "f", "b", "r", "d", "l", "f", "f", "r"};

    for (const string &command : commands)
    {
        if (command == "f")
        {
            cout << "Turning f\n";
            chandrayaan3.moveForward();
            chandrayaan3.printPositionAndface();
        }
        else if (command == "b")
        {
            cout << "Turning b\n";
            chandrayaan3.moveBackward();
            chandrayaan3.printPositionAndface();
        }
        else if (command == "r")
        {
            cout << "Turning r\n";
            chandrayaan3.turnRight();
            chandrayaan3.printPositionAndface();
        }
        else if (command == "l")
        {
            cout << "Turning l\n";
            chandrayaan3.turnLeft();
            chandrayaan3.printPositionAndface();
        }
        else if (command == "u")
        {
            cout << "Turning u\n";
            chandrayaan3.turnUp();
            chandrayaan3.printPositionAndface();
        }

        else if (command == "d")
        {
            cout << "Turning d\n";
            chandrayaan3.turnDown();
            chandrayaan3.printPositionAndface();
        }
    }
    
    chandrayaan3.printPositionAndface();

    return 0;
}