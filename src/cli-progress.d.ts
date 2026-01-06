declare module 'cli-progress' {
  export class SingleBar {
    constructor(options: any, preset?: any);
    start(total: number, startValue: number, payload?: any): void;
    update(current: number, payload?: any): void;
    increment(delta?: number, payload?: any): void;
    stop(): void;
  }

  export class MultiBar {
    constructor(options: any, preset?: any);
    create(total: number, startValue: number, payload?: any): SingleBar;
    stop(): void;
  }

  export namespace Presets {
    export const shades_classic: any;
    export const shades_grey: any;
    export const rect: any;
    export const legacy: any;
  }

  export namespace Format {
    export function ValueFormat(v: number, options: any, type: string): string;
  }
}
