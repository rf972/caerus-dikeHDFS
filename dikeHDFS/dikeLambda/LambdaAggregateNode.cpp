#include "LambdaAggregateNode.hpp"

#include <array>
#include <string>

using namespace lambda;

AggregateNode::AggregateNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
: Node(pObject, dikeProcessorConfig, output) 
{
    Poco::JSON::Array::Ptr groupingArray = pObject->getArray("GroupingArray");
    groupingColumns.resize(groupingArray->size());
    groupingMap.resize(groupingArray->size());
    groupingMapSize = groupingMap.size();
    groupingColumnTypes.resize(groupingArray->size());
    for(int i = 0; i < groupingArray->size(); i++) {
        Poco::JSON::Object::Ptr grouping = groupingArray->getObject(i);
        if(grouping->has("ColumnReference")) {            
            groupingColumns[i] = grouping->getValue<std::string>("ColumnReference");
       } else {
            std::cout << "ColumnReference is missing!" << std::endl;
        }        
    }
    if(verbose){
        std::cout << "GroupingArray : ";
        for(int i = 0; i < groupingColumns.size(); i++){
            std::cout << groupingColumns[i] << " ";
        }
        std::cout << std::endl;
    }
    
    Poco::JSON::Array::Ptr aggregateArray = pObject->getArray("AggregateArray");
    aggregateColumns.resize(aggregateArray->size());
    aggregateOP.resize(aggregateArray->size());
    aggregateMap.resize(aggregateArray->size());
    aggregateMapSize = aggregateMap.size();
    aggregateColumnTypes.resize(aggregateArray->size());
    for(int i = 0; i < aggregateArray->size(); i++) {
        Poco::JSON::Object::Ptr aggregate = aggregateArray->getObject(i);        
        Poco::JSON::Object::Ptr expression = aggregate->getObject("Expression");
        aggregateColumns[i] = expression->getValue<std::string>("ColumnReference");
        std::string str = aggregate->getValue<std::string>("Aggregate");
        aggregateOP[i] = StrToOp(str);
    }
    if(verbose){
        std::cout << "AggregateArray : ";
        for(int i = 0; i < aggregateColumns.size(); i++){
            std::cout << OpToStr(aggregateOP[i]) << "(" << aggregateColumns[i] << ") ";
        }
        std::cout << std::endl;
    }
}

AggregateNode::~AggregateNode()
{
    for ( auto it = groupingHashMap.begin(); it != groupingHashMap.end(); ++it ) {
         if(it->second.size() > 1) {
            std::cout << "Warning !!! Hash collision detected ..." << std::endl;
        }
    }
}

int AggregateNode::StrToOp(std::string & str) 
{
    int op;
    if(str.compare("sum") == 0){
        op = _SUM;
    } else if(str.compare("count") == 0){
        op = _COUNT;
    } else if(str.compare("min") == 0){
        op = _MIN;
    }else if(str.compare("max") == 0){
        op = _MAX;
    } else {
        std::cout << "Uknown operation : " << str << std::endl;
    }       
    return op;
}

std::string AggregateNode::OpToStr(int op) 
{
    std::string str;
    switch(op) {
        case _SUM:
            str = "sum";
            break;
        case _COUNT:
            str = "count";
            break;
        case _MIN:
            str = "min";
            break;
        case _MAX:
            str = "max";
            break;
        default:
            std::cout << "Uknown operation : " << op << std::endl;
            str = "Uknown";
    }

    return str;
}

Frame * AggregateNode::NewFrame()
{
    Frame * frame = new Frame(this);
    int colIdx = 0;
    for(int g = 0; g < groupingColumns.size(); g++){
        Column * col = new Column(this, colIdx++, groupingColumns[g],  groupingColumnTypes[g]);
        col->Init();
        frame->Add(col);
    }
    for(int a = 0; a < aggregateColumns.size(); a++){
        std::string colName = OpToStr(aggregateOP[a]) + "(" + aggregateColumns[a] + ")";
        Column * col = new Column(this, colIdx++, colName,  aggregateColumnTypes[a]);
        col->Init();
        frame->Add(col);
    }
    return frame;
}

void AggregateNode::UpdateColumnMap(Frame * inFrame) 
{    
    for(int i = 0; i < inFrame->columns.size(); i++){
        for(int g = 0; g < groupingColumns.size(); g++){
            if(groupingColumns[g].compare(inFrame->columns[i]->name) == 0){
                groupingMap[g] = i;
                groupingColumnTypes[g] = inFrame->columns[i]->data_type;
                inFrame->columns[i]->useCount++;
            }
        }
        for(int a = 0; a < aggregateColumns.size(); a++){
            if(aggregateColumns[a].compare(inFrame->columns[i]->name) == 0){
                aggregateMap[a] = i;
                aggregateColumnTypes[a] = inFrame->columns[i]->data_type;
                inFrame->columns[i]->useCount++;
            }
        }        
    }
    Frame * outFrame = NewFrame();
    frameArray.push_back(outFrame);

    Node::UpdateColumnMap(outFrame);        
}

uint64_t AggregateNode::GetRowHash(Frame * frame, int row)
{
    uint64_t hash = 0;
    for(int i = 0; i < groupingMapSize; i++){
        int col = groupingMap[i];
        hash ^= frame->columns[col]->GetHash(row);
    }
    return hash;
}

int AggregateNode::AddGroup(Frame * inFrame, int row)
{
    int group_index = groupCount;
    int frame_index = group_index / Column::config::MAX_SIZE;

    if(frameArray.size() < frame_index + 1) {
        frameArray.push_back(NewFrame());
    }

    Frame * groupingFrame = frameArray[frame_index]; 
    uint64_t group_row = groupingFrame->columns[0]->row_count;

    // Copy grouping columns
    for(int i = 0; i < groupingMapSize; i++){
        groupingFrame->columns[i]->CopyRow(group_row, inFrame->columns[groupingMap[i]], row);
        groupingFrame->columns[i]->row_count++;
    }

    // Copy aggregate columns    
    for(int i = 0; i < aggregateMapSize; i++){
        int col_idx = i + groupingMapSize;
        groupingFrame->columns[col_idx]->CopyRow(group_row, inFrame->columns[aggregateMap[i]], row);
        groupingFrame->columns[col_idx]->row_count++;
    }

    // Update all relevant counters
    groupCount++;
    return group_index;
}

template<typename T>
void AggregateNode::_Aggregate(int op, T * src, int src_idx, T * dst, int dst_idx) 
{
    switch(op){
        case _SUM:
            dst[dst_idx] += src[src_idx];
            break;            
    }
}

void AggregateNode::AggregateColumns(int op, Column * src_col, int src_row, Column * dst_col, int dst_row )
{
    switch(dst_col->data_type) {
    case Column::DataType::INT64:
        _Aggregate(op, src_col->int64_values, src_row,  dst_col->int64_values, dst_row);
        break;
    case Column::DataType::DOUBLE:
        _Aggregate(op, src_col->double_values, src_row,  dst_col->double_values, dst_row);        
        break;
    }
}

void AggregateNode::AggregateGroup(uint64_t group_index, Frame * inFrame, int row)
{
    int frame_index = group_index / Column::config::MAX_SIZE;
    int row_index = group_index % Column::config::MAX_SIZE;
    Frame * groupingFrame = frameArray[frame_index];
    
    for(int i = 0; i < aggregateMapSize; i++) {
        int col_idx = i + groupingMapSize;
        AggregateColumns(aggregateOP[i], inFrame->columns[aggregateMap[i]], row,  groupingFrame->columns[col_idx], row_index);
    }
}

void AggregateNode::Init(int rowGroupIndex)
{
    done = false;
    
    groupingHashMap.clear();
    groupCount = 0;
    
    clearFramePool(); // This will simply drop our frames

    // Reinit frameArray
    for(int i = 0; i < frameArray.size(); i++){
        frameArray[i]->lastFrame = false;
        for(int col = 0; col < frameArray[i]->columns.size(); col++){
            frameArray[i]->columns[col]->row_count = 0;
        }
    }
}

bool AggregateNode::Step()
{
    //std::cout << "AggregateNode::Step " << stepCount << std::endl;
    if(done) { return done; }
    stepCount++;

    Frame * inFrame = getFrame();
    if(inFrame == NULL) {
        std::cout << "Input queue is empty " << std::endl;
        return done;
    }
    
    std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();    

    // Iterate over all rows in frame
    for(int row = 0; row < inFrame->columns[groupingMap[0]]->row_count ; row++) {
        // Calculate row hash
        uint64_t hash = GetRowHash(inFrame, row);        
        // Check if this hash present in our map
        auto group = groupingHashMap.find(hash);
        if(group != groupingHashMap.end()) { // We found group for our hash
            aggregateCount++;
            AggregateGroup(group->second[0], inFrame, row);
        } else { // This hash does not exists
            int group_index = AddGroup(inFrame, row);
            groupingHashMap[hash].push_back(group_index);
            //int frame_index = group_index / Column::config::MAX_SIZE;
            //int row_index = group_index - (frame_index * Column::config::MAX_SIZE);
            //std::cout << "Adding " << group_index << " : " << frameArray[frame_index]->columns[0]->int64_values[row_index] << std::endl;
        }
    }

    if(inFrame->lastFrame){        
        done = true;
        if(verbose){
            std::cout << "aggregateCount " << aggregateCount << std::endl;
        }
        for(int i = 0; i < frameArray.size(); i++){
            if(i == frameArray.size() - 1) { // Our last frame
                frameArray[i]->lastFrame = true;
            }
            nextNode->putFrame(frameArray[i]); // Send frame down to graph
        }
    }

    inFrame->Free();

    std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();
    runTime += t2 - t1;        

    return done;
}