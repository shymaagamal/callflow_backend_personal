import { AuthGuard } from './auth.guard';

// The project uses a class-based AuthGuard (not a functional guard). This
// replaces the stale auto-generated stub that imported a non-existent
// `authGuard` function and broke the whole test build.
describe('AuthGuard', () => {
  it('class is defined', () => {
    expect(AuthGuard).toBeTruthy();
  });
});
