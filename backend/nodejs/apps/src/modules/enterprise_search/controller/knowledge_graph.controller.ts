import { NextFunction, Response } from 'express';
import { Logger } from '../../../libs/services/logger.service';
import { AIServiceCommand } from '../../../libs/commands/ai_service/ai.service.command';
import { HttpMethod } from '../../../libs/enums/http-methods.enum';
import { AuthenticatedUserRequest } from '../../../libs/middlewares/types';
import { InternalServerError } from '../../../libs/errors/http.errors';
import { AppConfig } from '../../tokens_manager/config/config';
import { handleBackendError } from './es_controller';

type AIServiceResponse<T> = { statusCode: number; data: T };

const logger = Logger.getInstance({ service: 'KnowledgeGraphController' });
const AI_SERVICE_UNAVAILABLE_MESSAGE =
  'AI service is currently unavailable. Please try again later.';

/**
 * Edrak addition: proxies the query service's permission-filtered graph neighbourhood
 * (`GET /api/v1/knowledge-graph/neighborhood`) so the gateway is the only surface the UI needs.
 * The caller's bearer token is forwarded; the Python side re-authenticates and filters by it.
 */
export const knowledgeGraphNeighborhood =
  (appConfig: AppConfig) =>
  async (
    req: AuthenticatedUserRequest,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    try {
      const queryParams: Record<string, string> = {};
      for (const [key, value] of Object.entries(req.query)) {
        if (typeof value === 'string') queryParams[key] = value;
      }
      const aiCommand = new AIServiceCommand<unknown>({
        uri: `${appConfig.aiBackend}/api/v1/knowledge-graph/neighborhood`,
        method: HttpMethod.GET,
        headers: req.headers as Record<string, string>,
        queryParams,
      });

      let aiResponse: AIServiceResponse<unknown>;
      try {
        aiResponse = (await aiCommand.execute()) as AIServiceResponse<unknown>;
      } catch (error: unknown) {
        logger.error('knowledge-graph proxy failed', error);
        throw new InternalServerError(
          AI_SERVICE_UNAVAILABLE_MESSAGE,
          error instanceof Error ? error : undefined,
        );
      }
      if (aiResponse.statusCode !== 200) {
        throw handleBackendError(
          {
            response: { status: aiResponse.statusCode, data: aiResponse.data },
          },
          'Knowledge graph',
        );
      }
      res.status(200).json(aiResponse.data);
    } catch (error) {
      next(error);
    }
  };
