#include <string>
#include <list>
#include <ctime>
#include <stdio.h>
#include <errno.h>
#include <fstream>
#include "StockMarshal.h"
#include "args.h"
#include <dirent.h>

using namespace Borealis;

struct StockData
{
    time_t time;
    float price;
};

namespace data
{
    list<StockData> stock;
}

typedef StockData StockData;

void StockMarshal::receivedAggregate(AggregateTuple  *tuple)
{
    time_t currenttime = tuple->currenttime;
    struct tm *timeinfo;
    timeinfo = localtime(&currenttime);
    char buffer[10];
    strftime(buffer, 10, "%H:%M:%S", timeinfo);
    cout << "begin: " << buffer << " " << tuple->maxprice << "  end" << endl;
}

void StockMarshal::sentPacket()
{
    if (data::stock.empty())
    {
        return;
    }
    StockData one = data::stock.front();
    data::stock.pop_front();
    Packet tuple;
    tuple._data.currenttime = one.time;
    tuple._data.price = one.price;
    batchPacket(&tuple);
    string time = string(ctime(&(one.time)));
    time = time.substr(0, time.length() - 1);
    // cout << "in stream: " << time << endl;
    sendPacket(1);
}

void parseRecord(string record, time_t *mytime, float *price)
{
    if (record.length() < 12)
    {
        cout << "Error format: " << record << endl;
        return;
    }

    int hh;
    int mm;
    int ss;
   
    // 14:59:13	12.91	--	64	82624	买盘
    hh = atoi(record.substr(0, 2).c_str());
    mm = atoi(record.substr(3, 2).c_str());
    ss = atoi(record.substr(6, 2).c_str());

    struct tm *t;
    time_t tmp_time = time(NULL);
    t = gmtime(&tmp_time);

    t->tm_hour = hh;
    t->tm_min = mm;
    t->tm_sec = ss;
    *mytime = mktime(t);

    string tmp = record.substr(9, record.length() - 9);
    *price = atof(tmp.substr(0, tmp.find_first_of('\t') + 1).c_str());
}

void processRead(string filename)
{
    filename = string("./data/") + filename;
    ifstream stream(filename.c_str(), ifstream::in);
    if (stream.good())
    {
        char title[100];
        stream.getline(title, 100);
        cout << title <<  endl;
    }
    else
    {
        perror(filename.c_str());
        exit(errno);
    }
    int tupleCount = 0;
    while (stream.good())
    {
        char other[100];
        stream.getline(other, 100);
        
        time_t time = 0;
        float price = 0;
        parseRecord(string(other), &time, &price);
        if (time == 0)
        {
            continue;
        }
        struct StockData s;
        s.time = time;
        s.price = price;
        data::stock.push_back(s);
        tupleCount++;
        if (tupleCount > 100)
        {
            break;
        }
    }
    data::stock.reverse();

    list<StockData>::iterator iter = data::stock.begin();
    int count = 0;
    for (; iter != data::stock.end(); iter++)
    {        
        time_t currenttime = iter->time;
        struct tm *timeinfo;
        timeinfo = localtime(&currenttime);
        char buffer[10];
        strftime(buffer, 10, "%H:%M:%S", timeinfo);        
        cout << buffer << " " << iter->price << endl;
        count++;
        if (count > 100)
        {
            break;
        }
    }

    stream.close();
}

void readStock()
{
    DIR *dir = NULL;
    dir = opendir("./data");
    if (dir == NULL)
    {
        puts("this is not a directory");
    }

    struct dirent *ent = NULL;
    ent = readdir(dir);
    while (ent != NULL)
    {
        if (ent->d_type == DT_REG && !strchr(ent->d_name, '~'))
        {
            puts(ent->d_name);
            processRead(string(ent->d_name));
        }
        ent = readdir(dir);
    }
    closedir(dir);
}

int main()
{
    int32 status;
    StockMarshal marshal;

    readStock();

    list<StockData>::iterator iter = data::stock.begin();

    status = marshal.open();

    if (status)
    {
        WARN << "Could not deploy network";
    }
    else
    {
        marshal.sentPacket();

        marshal.runClient();
    }

    return status;
}
