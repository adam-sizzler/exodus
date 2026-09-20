# Exodus

## Генерация Документации API (Swagger / OpenAPI / Scalar)

Exodus использует генератор документации **Swag** на основе декларативных аннотаций в Go-коде хэндлеров.

### Где находятся спецификации:
- `backend/internal/httpapi/panelsettings/docs/swagger.json`
- `backend/internal/httpapi/panelsettings/docs/swagger.yaml`

### Как добавить аннотацию к новому эндпоинту:
Над функцией хэндлера в `backend/internal/httpapi/<module>/` укажите Swagger-комментарии, например:
```go
// @Summary      Get users list
// @Description  Return paginated users stream
// @Tags         Users Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Router       /users [get]
```

### Команда генерации документации:
Из директории `backend/`:
```bash
cd backend
go generate ./...
```
*(Либо напрямую через утилиту `swag`)*:
```bash
swag init --generalInfo main.go --dir . --output internal/httpapi/panelsettings/docs --outputTypes json,yaml --parseDependency --parseInternal
```

Сгенерированный `swagger.json` автоматически встраивается в бинарник через `//go:embed docs/swagger.json` и сразу отображается на `/api/backend-tools/swagger` и `/api/backend-tools/scalar`.

---

Frontend audit и сборка:

```bash
cd frontend
npm run cb          # Полная проверка типов и сборка (contracts + frontend)
```

---

## Работа с Контрактами (@exodus/backend-contract)

Библиотека контрактов содержит Zod-схемы валидации, DTO и интерфейсы команд API, используемые фронтендом.

### Где находятся исходники:
`frontend/vendor/@exodus/backend-contract/`
- `models/` — схемы данных (Zod)
- `commands/` — команды запросов к API
- `constants/` — константы маршрутов, ошибок и ролей

### Как вносить изменения и пересобирать:
1. Внесите правки в TypeScript-файлы схем или команд в директории `frontend/vendor/@exodus/backend-contract/`.
2. Запустите сборку контракта:
   ```bash
   cd frontend/vendor/@exodus/backend-contract
   npm run build
   ```
   *Скрипт автоматически запустит `tsc` для бэкенд- и фронтенд-таргетов и сгенерирует актуальные `.d.ts` и `.js` бандлы в `build/`.*
3. Фронтенд (`frontend/src`) сразу получит обновлённую типизацию без ручного редактирования `dist` / `build`.

### Как генерировать/собирать контракты:

Команда для сборки контрактов запускается либо из корня пакета контрактов:
```bash
cd frontend/vendor/@exodus/backend-contract
npm run build
```

Либо напрямую из папки frontend:
```bash
cd frontend
npm run build:contract
```
