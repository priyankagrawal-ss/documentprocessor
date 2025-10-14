package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.model.ZipMaster;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ZipMasterRepository extends JpaRepository<ZipMaster, Long> {
}