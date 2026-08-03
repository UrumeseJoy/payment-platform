package com.paymentplatform.orchestration.controller;

import com.paymentplatform.ledger.service.ReconciliationService;
import com.paymentplatform.ledger.service.ReconciliationSummary;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/reconciliation")
public class ReconciliationController {

    private final ReconciliationService reconciliationService;

    public ReconciliationController(ReconciliationService reconciliationService) {
        this.reconciliationService = reconciliationService;
    }

    @PostMapping("/run")
    public ReconciliationSummary run() {
        return reconciliationService.runReconciliation();
    }
}
