package uk.gov.ons.ctp.response.action.service;

import static uk.gov.ons.ctp.common.time.DateTimeUtil.nowUTC;

import com.godaddy.logging.Logger;
import com.godaddy.logging.LoggerFactory;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import net.sourceforge.cobertura.CoverageIgnore;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import uk.gov.ons.ctp.common.error.CTPException;
import uk.gov.ons.ctp.common.state.StateTransitionManager;
import uk.gov.ons.ctp.common.time.DateTimeUtil;
import uk.gov.ons.ctp.response.action.domain.model.Action;
import uk.gov.ons.ctp.response.action.domain.model.ActionPlan;
import uk.gov.ons.ctp.response.action.domain.model.ActionPlanJob;
import uk.gov.ons.ctp.response.action.domain.model.ActionType;
import uk.gov.ons.ctp.response.action.domain.repository.ActionCaseRepository;
import uk.gov.ons.ctp.response.action.domain.repository.ActionPlanJobRepository;
import uk.gov.ons.ctp.response.action.domain.repository.ActionPlanRepository;
import uk.gov.ons.ctp.response.action.domain.repository.ActionRepository;
import uk.gov.ons.ctp.response.action.domain.repository.ActionRuleRepository;
import uk.gov.ons.ctp.response.action.domain.repository.ActionTypeRepository;
import uk.gov.ons.ctp.response.action.message.feedback.ActionFeedback;
import uk.gov.ons.ctp.response.action.representation.ActionDTO;
import uk.gov.ons.ctp.response.action.representation.ActionDTO.ActionEvent;
import uk.gov.ons.ctp.response.action.representation.ActionDTO.ActionState;

/**
 * An ActionService implementation which encapsulates all business logic operating on the Action
 * entity model.
 */
@Service
public class ActionService {
  private static final Logger log = LoggerFactory.getLogger(ActionService.class);

  private static final int TRANSACTION_TIMEOUT = 30;
  private static final String SYSTEM = "SYSTEM";

  private ActionRepository actionRepo;
  private ActionCaseRepository actionCaseRepo;
  private ActionPlanRepository actionPlanRepository;
  private ActionPlanJobRepository actionPlanJobRepository;
  private ActionRuleRepository actionRuleRepo;
  private ActionTypeRepository actionTypeRepo;
  public static final String ACTION_NOT_FOUND = "Action not found for id %s";

  private StateTransitionManager<ActionState, ActionDTO.ActionEvent>
      actionSvcStateTransitionManager;

  public ActionService(
      ActionRepository actionRepo,
      ActionCaseRepository actionCaseRepo,
      ActionPlanRepository actionPlanRepository,
      ActionPlanJobRepository actionPlanJobRepository,
      ActionRuleRepository actionRuleRepo,
      ActionTypeRepository actionTypeRepo,
      StateTransitionManager<ActionState, ActionDTO.ActionEvent> actionSvcStateTransitionManager) {
    this.actionRepo = actionRepo;
    this.actionCaseRepo = actionCaseRepo;
    this.actionPlanRepository = actionPlanRepository;
    this.actionPlanJobRepository = actionPlanJobRepository;
    this.actionRuleRepo = actionRuleRepo;
    this.actionTypeRepo = actionTypeRepo;
    this.actionSvcStateTransitionManager = actionSvcStateTransitionManager;
  }

  @CoverageIgnore
  public List<Action> findAllActionsOrderedByCreatedDateTimeDescending() {
    log.debug("Entering findAllActions");
    return actionRepo.findAllByOrderByCreatedDateTimeDesc();
  }

  @CoverageIgnore
  public List<Action> findActionsByTypeAndStateOrderedByCreatedDateTimeDescending(
      final String actionTypeName, final ActionDTO.ActionState state) {
    log.with("action_type_name", actionTypeName)
        .with("state", state)
        .debug("Entering findActionsByTypeAndState");
    return actionRepo.findByActionTypeNameAndStateOrderByCreatedDateTimeDesc(actionTypeName, state);
  }

  @CoverageIgnore
  public List<Action> findActionsByType(final String actionTypeName) {
    log.with("action_type_name", actionTypeName).debug("Entering findActionsByType");
    return actionRepo.findByActionTypeNameOrderByCreatedDateTimeDesc(actionTypeName);
  }

  @CoverageIgnore
  public List<Action> findActionsByState(final ActionDTO.ActionState state) {
    log.with("state", state).debug("Entering findActionsByState");
    return actionRepo.findByStateOrderByCreatedDateTimeDesc(state);
  }

  @CoverageIgnore
  public Action findActionById(final UUID actionId) {
    log.with("action_id", actionId).debug("Entering findActionById");
    return actionRepo.findById(actionId);
  }

  @CoverageIgnore
  public List<Action> findActionsByCaseId(final UUID caseId) {
    log.with("case_id", caseId).debug("Entering findActionsByCaseId");
    return actionRepo.findByCaseIdOrderByCreatedDateTimeDesc(caseId);
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      readOnly = false,
      timeout = TRANSACTION_TIMEOUT)
  public List<Action> cancelActions(final UUID caseId) throws CTPException {
    log.with("case_id", caseId).debug("Entering cancelAction");

    final List<Action> flushedActions = new ArrayList<>();
    final List<Action> actions = actionRepo.findByCaseId(caseId);
    for (final Action action : actions) {
      if (action.getActionType().getCanCancel()) {
        log.with("action_id", action.getId())
            .with("action_type", action.getActionType().getName())
            .debug("Cancelling action");
        final ActionDTO.ActionState nextState =
            actionSvcStateTransitionManager.transition(
                action.getState(), ActionEvent.REQUEST_CANCELLED);
        action.setState(nextState);
        action.setUpdatedDateTime(nowUTC());
        actionRepo.saveAndFlush(action);
        flushedActions.add(action);
      }
    }
    return flushedActions;
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      readOnly = false,
      timeout = TRANSACTION_TIMEOUT)
  public Action feedBackAction(final ActionFeedback actionFeedback) throws CTPException {
    final String actionId = actionFeedback.getActionId();
    log.with("action_id", actionId).debug("Entering feedBackAction");

    Action result = null;
    if (!StringUtils.isEmpty(actionId)) {
      result = actionRepo.findById(UUID.fromString(actionId));
      if (result != null) {
        final ActionDTO.ActionEvent event =
            ActionDTO.ActionEvent.valueOf(actionFeedback.getOutcome().name());
        result.setSituation(actionFeedback.getSituation());
        result.setUpdatedDateTime(nowUTC());
        final ActionDTO.ActionState nextState =
            actionSvcStateTransitionManager.transition(result.getState(), event);
        result.setState(nextState);
        result = actionRepo.saveAndFlush(result);
      }
    }

    return result;
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      readOnly = false,
      timeout = TRANSACTION_TIMEOUT)
  public Action createAdHocAction(final Action action) {
    log.debug("Creating adhoc action");

    // guard against the caller providing an id - we would perform an update otherwise
    action.setActionPK(null);

    // the incoming action has a placeholder action type with the name as provided to the caller but
    // we need the entire
    // action type object for that action type name
    final ActionType actionType = actionTypeRepo.findByName(action.getActionType().getName());
    action.setActionType(actionType);

    action.setManuallyCreated(true);
    action.setCreatedDateTime(nowUTC());
    action.setState(ActionState.SUBMITTED);
    action.setId(UUID.randomUUID());
    return actionRepo.saveAndFlush(action);
  }

  @Transactional
  public void createScheduledActions(ActionPlan actionPlan, ActionPlanJob actionPlanJob) {
   
    log.debug("createScheduledActions for plan " + actionPlan.getActionPlanPK());
    int result = actionCaseRepo.createActionsForPlan(actionPlan.getActionPlanPK());
    
    updatePlanAndJob(actionPlan, actionPlanJob);
  }
 
  private void updatePlanAndJob(ActionPlan actionPlan, ActionPlanJob actionPlanJob) {
    final Timestamp currentTime = nowUTC();
    actionPlanJob.complete(currentTime);
    actionPlan.setLastRunDateTime(currentTime);
    actionPlanJobRepository.saveAndFlush(actionPlanJob);
    actionPlanRepository.saveAndFlush(actionPlan);
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      readOnly = false,
      timeout = TRANSACTION_TIMEOUT)
  public Action updateAction(final Action action) {
    final UUID actionId = action.getId();
    log.with("action_id", actionId).debug("Entering updateAction");
    Action existingAction = actionRepo.findById(actionId);
    if (existingAction != null) {
      boolean needsUpdate = false;

      final Integer newPriority = action.getPriority();
      log.with("new_priority", newPriority).debug("Got priority");
      if (newPriority != null) {
        needsUpdate = true;
        existingAction.setPriority(newPriority);
      }

      final String newSituation = action.getSituation();
      log.with("new_situation", newSituation).debug("Got situation");
      if (newSituation != null) {
        needsUpdate = true;
        existingAction.setSituation(newSituation);
      }

      if (needsUpdate) {
        existingAction.setUpdatedDateTime(nowUTC());
        log.with("existing_action", existingAction).debug("updating action");
        existingAction = actionRepo.saveAndFlush(existingAction);
      }
    }
    return existingAction;
  }

  @Transactional(rollbackFor = Exception.class)
  public void rerunAction(final List<UUID> actionIds) throws CTPException {
    for (UUID actionId : actionIds) {
      Action actionToReRun = actionRepo.findById(actionId);
      log.with("action_id", actionId.toString()).debug("Rerunning Aborted Action");

      if (actionToReRun == null) {
        throw new CTPException(
            CTPException.Fault.RESOURCE_NOT_FOUND, ACTION_NOT_FOUND, actionId.toString());
      }

      actionToReRun.setUpdatedDateTime(DateTimeUtil.nowUTC());
      final ActionDTO.ActionState nextState =
          actionSvcStateTransitionManager.transition(
              actionToReRun.getState(), ActionEvent.REQUEST_RERUN);
      actionToReRun.setState(nextState);
      actionRepo.saveAndFlush(actionToReRun);
    }
  }
}
