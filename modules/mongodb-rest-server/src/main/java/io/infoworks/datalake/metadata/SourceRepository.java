package io.infoworks.datalake.metadata;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface SourceRepository extends MongoRepository<Source, String> {
    public Source findBySourceName(String name);


}
