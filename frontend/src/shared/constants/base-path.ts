const normalizeBasePath = (rawBasePath: string): string => {
    const value = rawBasePath.trim()

    if (value === '' || value === '/') {
        return ''
    }

    const withoutSlashes = value.replace(/^\/+|\/+$/g, '')
    if (withoutSlashes === '') {
        return ''
    }

    return `/${withoutSlashes}`
}

const resolveInitialBasePath = (): string => {
    if (typeof window === 'undefined') {
        return ''
    }
    if (window.__EXODUS_RUNTIME__?.basePath) {
        return normalizeBasePath(window.__EXODUS_RUNTIME__.basePath)
    }
    const baseEl = document.querySelector('base')
    if (baseEl) {
        const href = baseEl.getAttribute('href')
        if (href && href !== '/') {
            return normalizeBasePath(href)
        }
    }
    const pathname = window.location.pathname
    if (pathname && pathname !== '/') {
        const firstSegment = pathname.split('/').filter(Boolean)[0]
        if (
            firstSegment &&
            !['api', 'assets', 'favicons', 'lotties', 'splash_screens'].includes(firstSegment)
        ) {
            return `/${firstSegment}`
        }
    }
    return ''
}

export const APP_BASE_PATH = resolveInitialBasePath()
export const APP_BASE_PATH_WITH_TRAILING_SLASH =
    APP_BASE_PATH === '' ? '/' : `${APP_BASE_PATH}/`

export const getAppBasePath = (): string => {
    if (typeof window !== 'undefined') {
        if (window.__EXODUS_RUNTIME__?.basePath) {
            return normalizeBasePath(window.__EXODUS_RUNTIME__.basePath)
        }
        const baseEl = document.querySelector('base')
        if (baseEl) {
            const href = baseEl.getAttribute('href')
            if (href && href !== '/') {
                return normalizeBasePath(href)
            }
        }
        const pathname = window.location.pathname
        if (pathname && pathname !== '/') {
            const firstSegment = pathname.split('/').filter(Boolean)[0]
            if (
                firstSegment &&
                !['api', 'assets', 'favicons', 'lotties', 'splash_screens'].includes(firstSegment)
            ) {
                return `/${firstSegment}`
            }
        }
    }
    return APP_BASE_PATH
}

export const withBasePath = (path: string): string => {
    if (!path.startsWith('/')) {
        return path
    }

    const basePath = getAppBasePath()
    if (basePath === '') {
        return path
    }

    if (path === basePath || path.startsWith(`${basePath}/`)) {
        return path
    }

    return `${basePath}${path}`
}
