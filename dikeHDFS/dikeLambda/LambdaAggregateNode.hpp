#ifndef LAMBDA_AGGREGATE_NODE_HPP
#define LAMBDA_AGGREGATE_NODE_HPP

#include <unordered_map>

#include "LambdaNode.hpp"

namespace lambda {

class Aggregate;
class Grouping;

class AggregateNode : public Node {
    enum AggregateOperation {
        _MIN = 1, 
        _MAX = 2, 
        _COUNT = 3, 
        _SUM = 4, 
    };
    public:
    int totalRowGroups = 0; // Total row groups in file
    int rowGroupCounter = 0; // Barrier node will have to count "done" frames
    std::vector<std::string> groupingColumns;
    std::vector<int> groupingMap;
    int groupingMapSize = 0;
    std::vector<Column::DataType> groupingColumnTypes;

    std::vector<std::string> aggregateColumns;
    std::vector<int> aggregateMap;
    int aggregateMapSize = 0;
    std::vector<Column::DataType> aggregateColumnTypes;
    std::vector<int> aggregateOP;

    std::vector<Frame *> frameArray;
    std::unordered_map< uint64_t, int > groupingHashMap;

    // This is temporary hack to evaluate min / max approach
    uint64_t groupingHashMapMin = std::numeric_limits<uint64_t>::max();
    uint64_t groupingHashMapMax = 0;

    int groupCount = 0; // Number of valid groups

    // Stats
    uint64_t aggregateCount = 0; // How many rows we aggregated
    
    AggregateNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);
    ~AggregateNode();

    int StrToOp(std::string & str);
    std::string OpToStr(int op);

    Frame * NewFrame();
    uint64_t GetRowHash(Frame * frame, int row);
    int  AddGroup(Frame * frame, int row); // Returns group index
    void AggregateGroup(uint64_t group_index, Frame * inFrame, int row);
    void AggregateColumns(int op, Column * src_col, int src_row, Column * dst_col, int dst_row );
    template<typename T> void _Aggregate(int op, T * src, int src_idx, T * dst, int dst_idx);

    virtual void UpdateColumnMap(Frame * frame) override;
    virtual bool Step() override;
    virtual void Init(int rowGroupIndex) override;
};

} // namespace lambda

#endif /* LAMBDA_AGGREGATE_NODE_HPP */